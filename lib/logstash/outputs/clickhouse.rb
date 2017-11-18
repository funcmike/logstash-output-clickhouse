# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"
require "uri"
require "stud/buffer"
require "logstash/plugin_mixins/http_client"
require "securerandom"


class LogStash::Outputs::ClickHouse < LogStash::Outputs::Base
  include LogStash::PluginMixins::HttpClient
  include Stud::Buffer

  config_name "clickhouse"

  config :http_hosts, :validate => :array, :required => true

  config :table, :validate => :string, :required => true
  
  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]`
  config :headers, :validate => :hash

  config :flush_size, :validate => :number, :default => 50

  config :idle_flush_time, :validate => :number, :default => 5

  config :pool_max, :validate => :number, :default => 50

  config :save_on_failure, :validate => :boolean, :default => true

  config :save_dir, :validate => :string, :default => "/tmp"

  config :request_tolerance, :validate => :number, :default => 5
  
  config :backoff_time, :validate => :number, :default => 3

  config :mutations, :validate => :hash, :default => {}

  def register
    # Handle this deprecated option. TODO: remove the option
    #@ssl_certificate_validation = @verify_ssl if @verify_ssl

    # We count outstanding requests with this queue
    # This queue tracks the requests to create backpressure
    # When this queue is empty no new requests may be sent,
    # tokens must be added back by the client on success
    @request_tokens = SizedQueue.new(@pool_max)
    @pool_max.times {|t| @request_tokens << true }
    @requests = Array.new
    @http_query = "/?query=INSERT%20INTO%20#{table}%20FORMAT%20JSONEachRow"


    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
    logger.info("Initialized clickhouse with settings", 
      :flush_size => @flush_size,
      :idle_flush_time => @idle_flush_time,
      :request_tokens => @pool_max,
      :http_hosts => @http_hosts,
      :http_query => @http_query,
      :headers => request_headers)

  end # def register

  # This module currently does not support parallel requests as that would circumvent the batching
  def receive(event, async_type=:background)
    buffer_receive(event)
  end #def event

  public
  def mutate(event)
    h = event.to_hash()
    r = {}
    @mutations.each_pair do |key, mutation|
      next unless h.key?(mutation)
      case mutation
      when String then
        r[key] = h[mutation]
      when Array then
        mutation, re = mutation
        r[key] = re.match(h[mutation])[1]
      end

      end
    end
    r
  end

  def flush(events, close=false)
    documents = ""  #this is the string of hashes that we push to Fusion as documents

    events.each do |event|
        documents << LogStash::Json.dump(mutate(event)) << "\n"
    end

    hosts = []
    http_hosts.each{|host| hosts << host.dup}

    make_request(documents, hosts, @http_query)
  end

  def multi_receive(events)
    events.each {|event| buffer_receive(event)}
  end

  private

  def save_to_disk(file_name, documents)
    begin
      file = File.open("#{save_dir}/#{table}_#{file_name}.json", "w")
      file.write(documents) 
    rescue IOError => e
      log_failure("An error occurred while saving file to disk: #{e}",
                    :file_name => file_name)
    ensure
      file.close unless file.nil?
    end
  end

  private

  def make_request(documents, hosts, query, con_count = 1, req_count = 1, host = "", uuid = SecureRandom.hex)

    if host == ""
      host = hosts.pop
    end

    url = host+query

    # Block waiting for a token
    #@logger.info("Requesting token ", :tokens => request_tokens.length())
    token = @request_tokens.pop
    @logger.debug("Got token", :tokens => @request_tokens.length)

    # Create an async request
    begin
      request = client.send(:post, url, :body => documents, :headers => request_headers, :async => true)
    rescue Exception => e
      @logger.warn("An error occurred while indexing: #{e.message}")
    end

    # attach handlers before performing request
    request.on_complete do
      # Make sure we return the token to the pool
      @request_tokens << token
    end

    request.on_success do |response|
      if response.code == 200
        @logger.debug("Successfully submitted", 
          :size => documents.length,
          :response_code => response.code,
          :uuid => uuid)
      else
        if req_count >= @request_tolerance
          log_failure(
              "Encountered non-200 HTTP code #{response.code}",
              :response_code => response.code,
              :url => url,
              :size => documents.length,
              :uuid => uuid)
          if @save_on_failure
            save_to_disk(uuid, documents)
          end
        else
          logger.info("Retrying request", :url => url)
          sleep req_count*@backoff_time
          make_request(documents, hosts, query, con_count, req_count+1, host, uuid)
        end
      end
    end

    request.on_failure do |exception|
      if hosts.length == 0
          log_failure("Could not access URL",
            :url => url,
            :method => @http_method,
            :headers => headers,
            :message => exception.message,
            :class => exception.class.name,
            :backtrace => exception.backtrace,
            :size => documents.length,
            :uuid => uuid)
          if @save_on_failure
            save_to_disk(uuid, documents)
          end
          return
      end
  
      if con_count >= @automatic_retries
        host = ""
        con_count = 0
      end
      
      logger.info("Retrying connection", :url => url)
      sleep @backoff_time
      make_request(documents, hosts, query, con_count+1, req_count, host, uuid)
    end

    Thread.new do 
      client.execute!
    end
  end

  # This is split into a separate method mostly to help testing
  def log_failure(message, opts)
    @logger.error("[HTTP Output Failure] #{message}", opts)
  end

  def request_headers()
    headers = @headers || {}
    headers["Content-Type"] ||= "application/json"
    headers
  end

end
