module Riak
  class Client
    class Balancer
      class RoundRobin < Balancer
        def initialize(*a)
          super *a

          @i = -1
        end

        def host
          @i = (@i + 1) % @client.hosts.size
          @client.hosts[@i]
        end
      end
    end
  end
end
