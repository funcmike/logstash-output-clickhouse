require 'resolv'
require 'mini_cache'

class ShortNameResolver
  def initialize(ttl:, logger:)
    @ttl = ttl
    @store = MiniCache::Store.new
    @logger = logger
  end

  private
  def resolve_cached(shortname)
    @store.get_or_set(shortname) do
      addresses = resolve(shortname)
      raise "Bad shortname '#{shortname}'" if addresses.empty?
      MiniCache::Data.new(addresses, expires_in: @ttl)
    end
  end

  private
  def resolve(shortname)
    addresses = Resolv::DNS.open do |dns|
      dns.getaddresses(shortname).map { |r| r.to_s }
    end

    @logger.info("Resolved shortname '#{shortname}' to addresses #{addresses}")

    return addresses
  end

  public
  def get_address(shortname)
    return resolve_cached(shortname).sample
  end

  public
  def get_addresses(shortname)
    return resolve_cached(shortname)
  end
end
