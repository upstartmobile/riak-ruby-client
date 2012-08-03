require 'riak/util/translation'

warn Riak.t('deprecated.failed_request', :backtrace => "    "+caller.join("\n    "))

module Riak
  module Errors
    # Methods added to {Riak::Error} for backwards compatibility with
    # the deprecated FailedRequest class.
    module FailedRequestDeprecated
      include Util::Translation

      [:method, :expected, :code, :headers, :body].each do |m|
        define_method(m) do
          warn Riak.t('deprecated.fr_accessor',
                      :backtrace => "    "+caller.join("\n    "),
                      :method => m.to_s)
          nil
        end
      end

      # @deprecated Was originally used internally, now removed.
      # @return [true, false] whether the error response is in JSON
      def is_json?
        warn t('deprecated.fr_is_json', :backtrace => "    "+caller.join("\n    "))
        true
      end

      # @deprecated Use {Riak::Errors::NotFound} instead
      # @return [true,false] whether the error represents a "not found" response
      def not_found?
        warn t('deprecated.fr_not_found', :backtrace => "    "+caller.join("\n    "))
        Riak::Errors::NotFound === self
      end

      # @deprecated Use {Riak::Errors::ServerError} instead
      # @return [true,false] whether the error represents an internal
      #   server error
      def server_error?
        warn t('deprecated.fr_server_error', :backtrace => "    "+caller.join("\n    "))
        Riak::Errors::ServerError === self
      end
    end

    Error.send(:include, Errors::FailedRequestDeprecated)
  end

  ProtobuffsFailedRequest = HTTPFailedRequest = FailedRequest = Errors::Error
end
