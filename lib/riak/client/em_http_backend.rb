require 'riak/client/http_backend'
require 'riak/failed_request'

module Riak
  class Client

    class EmHTTPBackend < HTTPBackend

      def self.configured?
        begin
          require 'em-http'
          require 'eventmachine'
          require 'openssl'
          true
        rescue LoadError, NameError
          false
        end
      end

      # EM::HttpRequest has no persistent connection.
      def teardown; end

      private
      def perform( method, uri, headers, expect, data=nil )
        EventMachine.run do
          
          if data.is_a? String
            body_data = data
          end

          http_reqest = EventMachine::HttpRequest.new( uri ).send( method )
          
          http_request.errback{
            raise Riak::HTTPFailedRequest.new( method, expect, http_request.response_header.status.to_i, http_request.response.to_hash, http_request.response ) 
          }

          http_request.headers{ | hash | 
            unless valid_response?( expect, http_request.response_header.status.to_i )
              raise Riak::HTTPFailedRequest.new( method, expect, http_request.response_header.status.to_i, hash, http_request.response )
            end
          }

          http_request.stream{
            | chunk |
            p [ :data, chunk ]

          }

         # http_request.callback{
         #   result = {
         #     headers: http_request.response_header,
         #     code: http_request.response_header.status.to_i,
         #     body: http_request.response
         #   }
         # }

        end
      end

      def configure_connection
        

      end

    end #EMHTTPBackend class
  end #Client class
end #Riak module
