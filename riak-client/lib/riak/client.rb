
require 'tempfile'
require 'delegate'
require 'riak'
require 'riak/util/translation'
require 'riak/util/escape'
require 'riak/failed_request'
require 'riak/client/search'
require 'riak/client/http_backend'
require 'riak/client/net_http_backend'
require 'riak/client/excon_backend'
require 'riak/client/protobuffs_backend'
require 'riak/client/beefcake_protobuffs_backend'
require 'riak/client/host'
require 'riak/client/balancer'
require 'riak/client/balancer/round_robin'

module Riak
  # A client connection to Riak.
  class Client
    include Util::Translation
    include Util::Escape

    # When using integer client IDs, the exclusive upper-bound of valid values.
    MAX_CLIENT_ID = 4294967296

    # @return [String] The Riak::Hosts backing this client.
    attr_accessor :hosts
    attr_accessor :balancer

    # Creates a client connection to Riak
    #
    # Basic example:
    #     Riak::Client.new :host => 'riak1', :port => 8098
    #
    # Multiple hosts:
    #     Riak::Client.new :hosts => ['riak1', 'riak2'], :port => 8098
    #
    # With custom configuration for each host
    #     Riak::Client.new :hosts => [
    #       {:host => 'riak1', :port => 1234},
    #       {:host => 'riak2', :port => 5678}
    #     ]
    #
    # @param [Hash] options configuration options for the client. Merged with each host's configuration and passed to Riak::Client::Host.new.
    # @option options [String] :host '127.0.0.1' A single host to connect to.
    # @option options [Array] :hosts ['127.0.0.1'] An array consisting of a string hostname, or a partial options hash for Riak::Client::Host.
    # @option options [Symbol] :balancer :round_robin Which load balancer to use for host selection.
    def initialize(options={})
      self.client_id = options[:client_id] if options[:client_id]

      # Set up balancer.
      unless balancer_class = Riak::Client::Balancer[options[:balancer] || :round_robin]
        raise ArgumentError, "unknown balancer #{options[:balancer].inspect}"
      end
      self.balancer = balancer_class.new self

      # Set up hosts.
      hosts = [options[:host]] | (options[:hosts] || [])
      hosts.compact!
      if hosts.empty?
        hosts << "127.0.0.1"
      end

      self.hosts = hosts.map do |h|
        case h
        when Host
          h
        when String
          Host.new(options.merge(:host => h))
        when Hash
          Host.new(options.merge(h))
        else
          raise ArgumentError, "invalid host #{h.inspect}"
        end
      end
    end

    # Set the client ID for this client. Must be a string or Fixnum value 0 =< value < MAX_CLIENT_ID.
    # @param [String, Fixnum] value The internal client ID used by Riak to route responses
    # @raise [ArgumentError] when an invalid client ID is given
    # @return [String] the assigned client ID
    def client_id=(value)
      value = case value
              when 0...MAX_CLIENT_ID, String
                value
              else
                raise ArgumentError, t("invalid_client_id", :max_id => MAX_CLIENT_ID)
              end
      backend.set_client_id value if backend.respond_to?(:set_client_id)
      @client_id = value
    end

    def client_id
      @client_id ||= backend.respond_to?(:get_client_id) ? backend.get_client_id : make_client_id
    end

    # Retrieves a bucket from Riak.
    # @param [String] bucket the bucket to retrieve
    # @param [Hash] options options for retrieving the bucket
    # @option options [Boolean] :props (false) whether to retrieve the bucket properties
    # @return [Bucket] the requested bucket
    def bucket(name, options={})
      unless (options.keys - [:props]).empty?
        raise ArgumentError, "invalid options"
      end
      @bucket_cache ||= {}
      (@bucket_cache[name] ||= Bucket.new(self, name)).tap do |b|
        b.props if options[:props]
      end
    end
    alias :[] :bucket

    # Returns a backend for operations that are protocol-independent.
    # @return [HTTPBackend, ProtobuffsBackend] an appropriate host backend.
    def backend
      host.backend
    end

    # Returns an HTTP backend.
    # @return [HTTPBackend]
    def http
      host.http
    end

    # Returns a protobuffs backend.
    # @return [ProtobuffsBackend]
    def protobuffs
      host.protobuffs
    end

    # Lists buckets which have keys stored in them.
    # @note This is an expensive operation and should be used only
    #       in development.
    # @return [Array<Bucket>] a list of buckets
    def buckets
      host.buckets
    end
    alias :list_buckets :buckets

    # Asks the balancer for a host.
    # @return [Riak::Host] a Host for the next request.
    def host
      @balancer.host
    end

    # Stores a large file/IO-like object in Riak via the "Luwak" interface.
    # @overload store_file(filename, content_type, data)
    #   Stores the file at the given key/filename
    #   @param [String] filename the key/filename for the object
    #   @param [String] content_type the MIME Content-Type for the data
    #   @param [IO, String] data the contents of the file
    # @overload store_file(content_type, data)
    #   Stores the file with a server-determined key/filename
    #   @param [String] content_type the MIME Content-Type for the data
    #   @param [String, #read] data the contents of the file
    # @return [String] the key/filename where the object was stored
    def store_file(*a)
      host.store_file *a
    end

    # Retrieves a large file/IO object from Riak via the "Luwak"
    # interface. Streams the data to a temporary file unless a block
    # is given.
    # @param [String] filename the key/filename for the object
    # @return [IO, nil] the file (also having content_type and
    #   original_filename accessors). The file will need to be
    #   reopened to be read. nil will be returned if a block is given.
    # @yield [chunk] stream contents of the file through the
    #     block. Passing the block will result in nil being returned
    #     from the method.
    # @yieldparam [String] chunk a single chunk of the object's data
    def get_file(*a, &block)
      host.get_file *a, &block
    end

    # Deletes a file stored via the "Luwak" interface
    # @param [String] filename the key/filename to delete
    def delete_file(*a)
      host.delete_file *a
    end

    # Checks whether a file exists in "Luwak".
    # @param [String] key the key to check
    # @return [true, false] whether the key exists in "Luwak"
    def file_exists?(*a)
      host.file_exists? *a
    end
    alias :file_exist? :file_exists?

    # @return [String] A representation suitable for IRB and debugging output.
    def inspect
      "#<Riak::Client #{hosts.inspect}>"
    end

    private
    def make_client_id
      rand(MAX_CLIENT_ID)
    end

    # @private
    class LuwakFile < DelegateClass(Tempfile)
      attr_accessor :original_filename, :content_type
      alias :key :original_filename
      def initialize(fn)
        super(Tempfile.new(fn))
        @original_filename = fn
      end
    end
  end
end
