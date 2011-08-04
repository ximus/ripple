module Riak
  class Client
    class Balancer
      SUBCLASSES = {}

      # Look up a balancer class by symbol.
      def self.[](symbol)
        SUBCLASSES[symbol]
      end
      
      def self.inherited(klass)
        symbol = klass.to_s.split('::').last.gsub(/([a-z])([A-Z]+)/) { |m| 
          "#{m[0]}_#{m[1]}"
        }.downcase.to_sym

        SUBCLASSES[symbol] = klass
      end

      attr_reader :client

      def initialize(client)
        @client = client
      end

      def client
        @client
      end

      # Returns a host.
      def host
        # Implemented by subclasses.
      end
    end
  end
end
