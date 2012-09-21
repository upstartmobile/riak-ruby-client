module Riak
  module Errors
    # Raised when the requested key is not found
    class NotFound < Error
      def initialize(bucket=nil, key=nil)
        super t('not_found', :bucket => bucket, :key => key)
      end
    end

    # Raised when an unknown server-side error has occured.
    class ServerError < Error
    end

    # Raised when {Riak::RObject#prevent_stale_writes} is `true` and
    # the server reports a newer version
    class StaleWrite < Error
      def initialize
        super t('stale_write_prevented')
      end
    end

    # Raised when an aspect of the request was malformed
    class BadRequest < Error
    end

    # Raised when a request quorum is invalid.
    class InvalidQuorum < BadRequest
      def initialize(name)
        super t('invalid_quorum', :name => name)
      end
    end

    # Raised when a request quorum is greater than the bucket's
    # N-value.
    class NValViolation < BadRequest
      def initialize(n)
        super t('n_val_violation', :n => n)
      end
    end

    # Raised when an index query is invalid
    class InvalidIndexQuery < BadRequest
    end

    # Raised when a request times out internally to the Riak node
    class RequestTimedOut < Error
      def initialize
        super t('request_timed_out')
      end
    end

    # Raised when the requested PR or PW quorum cannot be met
    class InsufficientPrimaries < Error
      def initialize(q, p)
        super t('insufficient_primaries', :q => q, :p => p)
      end
    end

    # Raised when the requested PR or PW quorum cannot be met
    class InsufficientReplicas < Error
      def initialize(q, p)
        super t('insufficient_replicas', :q => q, :p => p)
      end
    end

    # Raised when a quorum is not met for the request
    class QuorumNotMet < Error
      def initialize(quorums)
        qstrings = quorums.map {|k,(rec,exp)| "#{k}=#{rec}/#{exp}" }
        super t('quorum_not_met', :quorum => qstrings.join(", "))
      end
    end

    # Raised when too many failures were received from a quorum of the
    # request.
    class QuorumFailed < Error
      def initialize
        super t('quorum_failure')
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
