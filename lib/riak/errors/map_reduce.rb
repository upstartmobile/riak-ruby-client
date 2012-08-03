require 'riak/errors/usage_errors'
require 'riak/errors/request'

module Riak
  module Errors
    module MapReduce
      # Raised when adding a key-filter to a {Riak::MapReduce} object
      # using the wrong number of arguments to the filter
      class FilterArityMismatch < InvalidArgument
        def initialize(filter, expected, received)
          super t('filter_arity_mismatch',
                  :filter => filter,
                  :expected => expected,
                  :received => received)
        end
      end

      # Raised when adding a key-filter to a {Riak::MapReduce} object
      # that requires a block but no block is given
      class FilterRequiresBlock < MissingBlock
        def initialize(op)
          super t('filter_needs_block', :filter => op)
        end
      end

      # Raised when an invalid phase type is given to
      # {Riak::MapReduce::Phase#type=}
      class InvalidPhaseType < InvalidArgument
        def initialize
          super t('invalid_phase_type')
        end
      end
      
      # Raised when an invalid function is given to {Riak::MapReduce::Phase#function=}
      class InvalidPhaseFunction < InvalidArgument; end

      # Raised when an invalid Erlang phase function is given
      class InvalidErlangPhase < InvalidPhaseFunction
        def initialize
          super t('module_function_pair_required')
        end
      end

      # Raised when an invalid stored function (bucket/key) is given
      class InvalidStoredFunction < InvalidPhaseFunction
        def initialize
          super t("stored_function_invalid")
        end
      end

      # Raised when a {Riak::WalkSpec} is given as the phase function,
      # but the phase type is not 'link'
      class NotLinkPhase < InvalidPhaseFunction
        def initialize
          super t('walk_spec_invalid_unless_link')
        end
      end

      # Raised when the phase function is of an unsupported type
      class UnknownFunctionType < InvalidPhaseFunction
        def initialize(function)
          super t("invalid_function_value", :value => function.inspect)
        end
      end
      
      # Raised when a {Riak::MapReduce} job is submitted without phases but
      # the server does not support phaseless-MapReduce
      class EmptyQuery < MapReduceError
        def initialize
          super t('empty_map_reduce_query')
        end
      end
    end
  end
end
