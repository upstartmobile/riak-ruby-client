require 'riak/util/translation'

module Riak
  # Namespace for Riak-related errors
  module Errors
    # The base class for all Riak-generated exceptions that aren't
    # descendants of other Exceptions
    class Error < ::StandardError
      include Util::Translation
    end
  end
  
  autoload :FailedRequest, 'riak/errors/failed_request_deprecated'
  autoload :HTTPFailedRequest, 'riak/errors/failed_request_deprecated'
  autoload :ProtobuffsFailedRequest, 'riak/errors/failed_request_deprecated'
  
  require 'riak/errors/request'  
  require 'riak/errors/usage'
  require 'riak/errors/map_reduce'
end
