require 'riak/errors'

module Riak
  class Client
    class HTTPBackend
      module ErrorHandling
        # Called when an unexpected response code is received so that
        # we can raise the appropriate semantic error.
        def handle_error(code, headers, body)
          case code
          when 400
            handle_malformed_request(headers, body)
          when 403
            handle_forbidden(headers, body)
          when 404
            handle_not_found(headers, body)
          when 503
            handle_unavailable(headers, body)
          when 500
            handle_server_error(headers, body)
          end
        end

        # Handles 400 response errors
        def handle_malformed_request(headers, body)
          case body
          when /query parameter/i
            raise InvalidQuorum, body.split(/\s/).first
          when /invalid for bucket/i
            raise NValViolation, body[/\d+/]
          when /missing content-Type request header/i
            raise ContentTypeMissing
          when /invalid query/i
            raise InvalidIndexQuery, body
          when /invalid link header|unknown field type|could not parse field/i
            raise BadRequest, body
          end
        end

        # Handles 403 response errors
        def handle_forbidden(headers, body)
          raise PrecommitFailed, body
        end

        # Handles 404 response errors
        def handle_not_found(headers, body)
          if @current_request == :fetch_object
            raise NotFound, *@current_request_args[0..1]
          end
        end

        # Handles 503 errors
        def handle_unavailable(headers, body)
          case body
          when /too many write failures/i
            raise QuorumFailed
          when /request timed out/i
            raise RequestTimedOut
          when /p[rw]-value unsatisfied: (\d+)\/(\d+)/i
            raise InsufficientPrimaries.new($2, $1)
          when /([rw])-value unsatisfied: (\d+)\/(\d+)/i
            raise QuorumNotMet, {$1 => [$2, $3]}
          when /unable to connect/i
            raise ServerError, body
          end
        end

        # Handles 500 errors
        def handle_server_error(headers, body)
          # We should just be able to raise Riak::Errors::Error, but
          # if there are new/edge-case errors unmunged by the WM
          # resources, we might be able to do something reasonable
          # here.
          if @current_request == :mapred
            raise MapReduceError, body
          end
          case body
          when /w_val_unsatisfied/
            # This is a special case unmatched in riak_kv_wm_object,
            # should normally be a 503
            vals = body.scan(/\d+/)
            raise QuorumNotMet, {:w => [vals[0], vals[2]],
              :dw => [vals[1], vals[3]] }
          when /insufficient_vnodes/
            raise InsufficientReplicas.new(*body.scan(/\d+/))
          else
            raise ServerError, body
          end
        end
      end
    end
  end
end
