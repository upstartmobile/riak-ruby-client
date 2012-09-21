require 'riak/client'
require 'riak/util/translation'

module Riak
  module Errors
    # Error raised when an argument is of the wrong type.
    class TypeMismatch < ArgumentError
      include Util::Translation

      class << self
        attr_accessor :klass
      end

      def self.expect!(value)
        raise new(value) if type_invalid?(value)
      end

      def self.type_invalid?(value)
         not klass === value
      end
    end

    # Raised when an argument was expected to be a {Riak::Client}
    # object.
    class ClientArgument < TypeMismatch
      self.klass = Riak::Client
      def initialize(not_client)
        super t('client_type', :client => not_client.inspect)
      end
    end

    # Raised when an argument was expected to be a
    # {Riak::Client::Node} object
    class NodeArgument < TypeMismatch
      self.klass = Riak::Client::Node

      def initialize(not_node)
        super t('node_type', :node => node.inspect)
      end
    end

    # Raised when an argument was expected to be a {String}
    class StringArgument < TypeMismatch
      self.klass = ::String

      def initialize(not_string)
        super t('string_type', :string => not_string.inspect)
      end
    end

    # Raised when an argument was expected to be a {Hash}
    class HashArgument < TypeMismatch
      self.klass = ::Hash
      def initialize(not_hash)
        super t('hash_type', :hash => not_hash.inspect)
      end
    end

    # Raised when an HTTP request body is not a {String} or {IO}-like
    class RequestBodyArgument < TypeMismatch
      self.klass = String

      def self.type_invalid?(value)
        super && !value.respond_to?(:read)
      end

      def initialize(not_body)
        super t('request_body_type')
      end
    end

    # Raised when an IO-like object is passed to
    # {Riak::RObject#data=}. IO objects are not serializable and
    # should be passed to {Riak::RObject#raw_data=} instead.
    class NonIOArgument < TypeMismatch
      def self.type_invalid?(value)
        value.respond_to?(:read)
      end

      def initialize(io)
        super t('invalid_io_object')
      end
    end

    # Raised when an argument does not meet the contract of the method
    class InvalidArgument < ArgumentError
      include Util::Translation
    end

    # Raised when a Secondary Index query is malformed
    class InvalidIndexQuery < InvalidArgument
      def initialize(query)
        super t('invalid_index_query', :value => query.inspect)
      end
    end

    # Raised when HTTP Basic auth is malformed
    class InvalidBasicAuth < InvalidArgument
      def initialize
        super t('invalid_basic_auth')
      end
    end

    # Raised when the given SSL verification mode is malformed
    class InvalidSSLVerifyMode < InvalidArgument
      def initialize(mode)
        t('invalid_ssl_verify_mode', :invalid => mode)
      end
    end

    # Raised when invalid Riak Search documents are given
    class InvalidSearchDoc < InvalidArgument
      def initialize
        super t('search_docs_require_id')
      end
    end

    # Raised when invalid Riak Search removal specifications are given
    class InvalidSearchRemove < InvalidArgument
      def initialize
        super t('search_remove_requires_id_or_query')
      end
    end

    # Raised when invalid options are given to {Riak::Client#initialize}
    class InvalidClientOptions < InvalidArgument
      def initialize(bad_options)
        super "#{bad_options.inspect} are not valid options for Riak::Client#initialize"
      end
    end

    # Raised when an invalid protocol is given
    class InvalidProtocol < InvalidArgument
      def initialize(bad_proto)
        super t('protocol_invalid', :invalid => bad_proto,
                :valid => Riak::Client::PROTOCOLS.join(', '))
      end
    end

    # Raised when an invalid client ID is given
    class InvalidClientID < InvalidArgument
      def initialize
        super t('invalid_client_id', :max_id => Riak::Client::MAX_CLIENT_ID)
      end
    end

    # Raised when {Riak::RObject#store} is called on an object with an
    # unset content-type.
    class ContentTypeMissing < InvalidArgument
      def initialize
        super t('content_type_undefined')
      end
    end

    # Raised when the method expects a block but no block is given.
    class MissingBlock < InvalidArgument
      def initialize(message=nil)
        super(message || t('missing_block'))
      end
    end

    # Raised when too few arguments are given to the method
    class InsufficientArguments < InvalidArgument
      def initialize(args)
        super t('too_few_arguments', :params => args)
      end
    end

    # Raised when a {Riak::WalkSpec} is initialized with incorrect
    # arguments
    class BadWalkSpec < InvalidArgument
      def initialize
        super t('wrong_argument_count_walk_spec')
      end
    end

    # Raised when attempting to serialize Ruby data into a content
    # type that has no serializer defined.
    class NoSerializer < NotImplementedError
      include Util::Translation
      def initialize(content_type)
        super t('serializer_not_implemented', :content_type => content_type.inspect)
      end
    end

    # Raised when a client backend is not available because of bad
    # configuration or a missing library
    class BackendConfiguration < Error
      def initialize(message, backend)
        super t(message, :backend => backend)
      end
    end

    # Raised when an HTTP backend is not available because of bad
    # configuration or a missing library
    class HTTPConfiguration < BackendConfiguration
      def initialize(backend)
        super 'http_configuration', backend
      end
    end

    # Raised when a Protobuffs backend is not available because of bad
    # configuration or a missing library
    class ProtobuffsConfiguration < BackendConfiguration
      def initialize(backend)
        super 'protobuffs_configuration', backend
      end
    end

    # Raised when the {Riak::Node} or {Riak::Cluster} input
    # configuration is missing required keys
    class MissingNodeConfiguration < InvalidArgument
      def initialize
        super t('source_and_root_required')
      end
    end

    # Raised when trying to convert a {Riak::Link} pointing to a
    # bucket to a {Riak::WalkSpec}
    class InvalidLinkConversion < Error
      def initialize
        super t('bucket_link_conversion')
      end
    end
  end
end
