# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"
require "logstash/util/shortname_resolver"
require "uri"
require "stud/buffer"
require "logstash/plugin_mixins/http_client"
require "securerandom"


class LogStash::Outputs::ClickHouse < LogStash::Outputs::Base
  include LogStash::PluginMixins::HttpClient
  include Stud::Buffer

  concurrency :single

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

  config :save_file, :validate => :string, :default => "failed.json"

  config :request_tolerance, :validate => :number, :default => 5
  
  config :backoff_time, :validate => :number, :default => 3

  config :automatic_retries, :validate => :number, :default => 3

  config :mutations, :validate => :hash, :default => {}

  config :host_resolve_ttl_sec, :validate => :number, :default => 120

  def print_plugin_info()
    @@plugins = Gem::Specification.find_all{|spec| spec.name =~ /logstash-output-clickhouse/ }
    @plugin_name = @@plugins[0].name
    @plugin_version = @@plugins[0].version
    @logger.info("Running #{@plugin_name} version #{@plugin_version}")

    @logger.info("Initialized clickhouse with settings",
      :flush_size => @flush_size,
      :idle_flush_time => @idle_flush_time,
      :request_tokens => @pool_max,
      :http_hosts => @http_hosts,
      :http_query => @http_query,
      :headers => request_headers)
  end

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

    @hostnames_pool =
      parse_http_hosts(http_hosts,
        ShortNameResolver.new(ttl: @host_resolve_ttl_sec, logger: @logger))

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )

    print_plugin_info()
  end # def register

  private

  def parse_http_hosts(hosts, resolver)
    ip_re = /^[\d]+\.[\d]+\.[\d]+\.[\d]+$/

    lambda {
      hosts.flat_map { |h|
        scheme = URI(h).scheme
        host = URI(h).host
        port = URI(h).port
        path = URI(h).path

        if ip_re !~ host
          resolver.get_addresses(host).map { |ip|
            "#{scheme}://#{ip}:#{port}#{path}"
          }
        else
          [h]
        end
      }
    }
  end

  private

  def get_host_addresses()
    begin
      @hostnames_pool.call
    rescue Exception => ex
      @logger.error('Error while resolving host', :error => ex.to_s)
    end
  end

  # This module currently does not support parallel requests as that would circumvent the batching
  def receive(event)
    buffer_receive(event)
  end

  def mutate( src )
    return src if @mutations.empty?
    res = {}
    @mutations.each_pair do |dstkey, source|
      case source
        when String then
          scrkey = source
          next unless src.key?(scrkey)

          res[dstkey] = src[scrkey]
        when Array then
          scrkey = source[0]
          next unless src.key?(scrkey)
          pattern = source[1]
          replace = source[2]
          res[dstkey] = src[scrkey].sub( Regexp.new(pattern), replace )
      end
    end
    res
  end

  public
  def flush(events, close=false)
    documents = ""  #this is the string of hashes that we push to Fusion as documents

    events.each do |event|
        documents << LogStash::Json.dump( mutate( event.to_hash() ) ) << "\n"
    end

    hosts = get_host_addresses()

    make_request(documents, hosts, @http_query, 1, 1, hosts.sample)
  end

  private

  def save_to_disk(documents)
    begin
      file = File.open("#{save_dir}/#{table}_#{save_file}", "a")
      file.write(documents) 
    rescue IOError => e
      log_failure("An error occurred while saving file to disk: #{e}",
                    :file_name => file_name)
    ensure
      file.close unless file.nil?
    end
  end

  def delay_attempt(attempt_number, delay)
    # sleep delay grows roughly as k*x*ln(x) where k is the initial delay set in @backoff_time param
    attempt = [attempt_number, 1].max
    timeout = lambda { |x| [delay*x*Math.log(x), 1].max }
    # using rand() to pick final sleep delay to reduce the risk of getting in sync with other clients writing to the DB
    sleep_time = rand(timeout.call(attempt)..timeout.call(attempt+1))
    sleep sleep_time
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

    request.on_success do |response|
      # Make sure we return the token to the pool
      @request_tokens << token

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
            save_to_disk(documents)
          end
        else
          @logger.info("Retrying request", :url => url, :message => response.message, :response => response.body, :uuid => uuid)
          delay_attempt(req_count, @backoff_time)
          make_request(documents, hosts, query, con_count, req_count+1, host, uuid)
        end
      end
    end

    request.on_failure do |exception|
      # Make sure we return the token to the pool
      @request_tokens << token

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
            save_to_disk(documents)
          end
          return
      end
  
      if con_count >= @automatic_retries
        host = ""
        con_count = 0
      end

      @logger.info("Retrying connection", :url => url, :uuid => uuid)
      delay_attempt(con_count, @backoff_time)
      make_request(documents, hosts, query, con_count+1, req_count, host, uuid)
    end

    client.execute!
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
