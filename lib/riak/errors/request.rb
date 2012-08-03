module Riak
  module Errors
    # Raised when the requested key is not found
    class NotFound < Error
    end

    # Raised when an unknown server-side error has occured.
    class ServerError < Error
    end

    # Raised when {Riak::RObject#prevent_stale_writes} is `true` and
    # the server reports a newer version
    class NotModified < Error
      def initialize
        super t('stale_write_prevented')
      end
    end

    # Raised when an error occurred in a MapReduce request.
    class MapReduceError < Error
    end

    # Raised when calling {Stamp#next} and NTP or some other external
    # event has moved the system clock backwards.
    class BackwardsClockError < Error
      def initialize(delay)
        super t('backwards_clock', :delay => delay)
      end
    end

    # Raised when a requested feature is not supported by the
    # connected Riak server.
    class FeatureUnsupported < Error      
    end

    # Raised when secondary indexes are unsupported
    class IndexesUnsupported < FeatureUnsupported
      def initialize
        super t('indexes_unsupported')
      end
    end

    # Raised when search is unsupported
    class SearchUnsupported < FeatureUnsupported
      def initialize
        super t('search_unsupported')
      end
    end

    # Raised when Luwak is unsupported
    class LuwakUnsupported < FeatureUnsupported
      def initialize
        super t('luwak_unsupported')
      end
    end
  end
end
