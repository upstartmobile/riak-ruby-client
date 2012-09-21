require 'base64'
require 'riak/json'
require 'riak/client'
require 'riak/errors'
require 'riak/client/protobuffs_backend'

module Riak
  class Client
    class BeefcakeProtobuffsBackend < ProtobuffsBackend
      include Errors

      def self.configured?
        begin
          require 'beefcake'
          require 'riak/client/beefcake/messages'
          require 'riak/client/beefcake/object_methods'
          true
        rescue LoadError, NameError
          false
        end
      end

      def ping
        @current_request = MESSAGE_CODES.index(:PingReq)
        socket.write([1, MESSAGE_CODES.index(:PingReq)].pack('NC'))
        decode_response {|res| res == :PingResp }
      end

      def get_client_id
        @current_request = MESSAGE_CODES.index(:GetClientIdReq)
        socket.write([1, MESSAGE_CODES.index(:GetClientIdReq)].pack('NC'))
        decode_response {|res| res.client_id }
      end

      def server_info
        @current_request = MESSAGE_CODES.index(:GetServerInfoReq)
        socket.write([1, MESSAGE_CODES.index(:GetServerInfoReq)].pack('NC'))
        decode_response do |res|
          {:node => res.node, :server_version => res.server_version}
        end
      end

      def list_buckets
        @current_request = MESSAGE_CODES.index(:ListBucketsReq)
        socket.write([1, MESSAGE_CODES.index(:ListBucketsReq)].pack('NC'))
        decode_response do |res|
          res == :ListBucketsResp ? [] : res.buckets
        end
      end

      def set_client_id(id)
        value = case id
                when Integer
                  [id].pack("N")
                else
                  id.to_s
                end
        req = RpbSetClientIdReq.new(:client_id => value)
        write_protobuff(:SetClientIdReq, req)
        decode_response {|res| res == :SetClientIdResp }
      end

      def fetch_object(bucket, key, options={})
        options = prune_unsupported_options(:GetReq, normalize_quorums(options))
        bucket = Bucket === bucket ? bucket.name : bucket
        req = RpbGetReq.new(options.merge(:bucket => maybe_encode(bucket), :key => maybe_encode(key)))
        write_protobuff(:GetReq, req)
        robject = RObject.new(client.bucket(bucket), key)
        decode_response do |res|
          if res == :GetResp
            raise NotFound.new(bucket, key)
          else
            load_object(res, robject)
          end
        end
      end

      def reload_object(robject, options={})
        options = normalize_quorums(options)
        options[:bucket] = maybe_encode(robject.bucket.name)
        options[:key] = maybe_encode(robject.key)
        options[:if_modified] = maybe_encode Base64.decode64(robject.vclock) if robject.vclock
        req = RpbGetReq.new(prune_unsupported_options(:GetReq, options))
        write_protobuff(:GetReq, req)
        decode_response do |res|
          if res == :GetResp
            raise NotFound.new(robject.bucket, robject.key)
          else
            load_object(res, robject)
          end
        end
      end

      def store_object(robject, options={})
        options = normalize_quorums(options)
        if robject.prevent_stale_writes
          unless pb_conditionals?
            other = fetch_object(robject.bucket, robject.key)
            raise NotModified unless other.vclock == robject.vclock
          end
          if robject.vclock
            options[:if_not_modified] = true
          else
            options[:if_none_match] = true
          end
        end
        req = dump_object(robject, prune_unsupported_options(:PutReq, options))
        write_protobuff(:PutReq, req)

        decode_response do |res|
          case res
          when RpbPutResp
            load_object(res, robject)
          when :PutResp
            true
          else
            decode_error(res)
          end
        end
      end

      def delete_object(bucket, key, options={})
        bucket = Bucket === bucket ? bucket.name : bucket
        options = normalize_quorums(options)
        options[:bucket] = maybe_encode(bucket)
        options[:key] = maybe_encode(key)
        options[:vclock] = Base64.decode64(options[:vclock]) if options[:vclock]
        req = RpbDelReq.new(prune_unsupported_options(:DelReq, options))
        write_protobuff(:DelReq, req)
        decode_response {|res| res == :DelResp }
      end

      def get_bucket_props(bucket)
        bucket = bucket.name if Bucket === bucket
        req = RpbGetBucketReq.new(:bucket => maybe_encode(bucket))
        write_protobuff(:GetBucketReq, req)
        decode_response do |res|
          {
            'n_val' => res.props.n_val,
            'allow_mult' => res.props.allow_mult
          }
        end
      end

      def set_bucket_props(bucket, props)
        bucket = bucket.name if Bucket === bucket
        props = props.slice('n_val', 'allow_mult')
        req = RpbSetBucketReq.new(:bucket => maybe_encode(bucket), :props => RpbBucketProps.new(props))
        write_protobuff(:SetBucketReq, req)
        decode_response {|res| res == :SetBucketResp }
      end

      # Thunk for decoding streaming operations
      K = proc { |v| v }

      def list_keys(bucket, &block)
        bucket = bucket.name if Bucket === bucket
        req = RpbListKeysReq.new(:bucket => maybe_encode(bucket))
        write_protobuff(:ListKeysReq, req)
        keys = []
        while msg = decode_response(&K)
          break if msg == :ListKeysResp || msg.done
          if block_given?
            yield msg.keys
          else
            keys += msg.keys
          end
        end
        block_given? || keys
      end

      def mapred(mr, &block)
        raise Errors::MapReduce::EmptyQuery if mr.query.empty? && !mapred_phaseless?
        req = RpbMapRedReq.new(:request => mr.to_json, :content_type => "application/json")
        write_protobuff(:MapRedReq, req)
        results = []
        while msg = decode_response(&K)
          break if msg.done
          if block_given?
            yield msg.phase, JSON.parse(msg.response)
          else
            results[msg.phase] ||= []
            results[msg.phase] += JSON.parse(msg.response)
          end
        end
        block_given? || results.compact.size == 1 ? results.last : results
      end

      def get_index(bucket, index, query)
        return super unless pb_indexes?
        if Range === query
          options = {
            :qtype => RpbIndexReq::IndexQueryType::RANGE,
            :range_min => query.begin.to_s,
            :range_max => query.end.to_s
          }
        else
          options = {
            :qtype => RpbIndexReq::IndexQueryType::EQ,
            :key => query.to_s
          }
        end
        req = RpbIndexReq.new(options.merge(:bucket => bucket, :index => index))
        write_protobuff(:IndexReq, req)
        decode_response {|res| res == :IndexResp ? [] : res.keys }
      end

      def search(index, query, options={})
        return super unless pb_search?
        options = options.symbolize_keys
        options[:op] = options.delete(:'q.op') if options[:'q.op']
        req = RpbSearchQueryReq.new(options.merge(:index => index || 'search', :q => query))
        write_protobuff(:SearchQueryReq, req)
        decode_response do |res|
          {
            'docs' => res.docs.map {|d| decode_doc(d) },
            'max_score' => res.max_score,
            'num_found' => res.num_found
          }
        end
      end

      private
      def write_protobuff(code, message)
        @current_request = code
        encoded = message.encode
        header = [encoded.length+1, MESSAGE_CODES.index(code)].pack("NC")
        socket.write(header + encoded)
      end

      def decode_response
        header = socket.read(5)
        raise SocketError, "Unexpected EOF on PBC socket" if header.nil?
        msglen, msgcode = header.unpack("NC")
        message = socket.read(msglen-1)
        if MESSAGE_CODES[msgcode] == :ErrorResp
          decode_error RpbErrorResp.decode(message)
        elsif MESSAGES[msgcode] && !message.empty?
          yield MESSAGES[msgcode].decode(message)
        else
          yield MESSAGE_CODES[msgcode]
        end
      rescue SystemCallError, SocketError => e
        reset_socket
        raise
      end

      def decode_doc(doc)
        Hash[doc.properties.map {|p| [ p.key, p.value ] }]
      end

      def decode_error(error)
        if @current_request == :MapRedReq
          raise MapReduceError, error.errmsg
        end
        case error.errmsg
        when /modified|match_found/ # on PutReq only
          raise StaleWrite
        when /notfound/ # on PutReq only
          raise NotFound
        when /(?:n|r|pr|w|dw|pw)_val_violation/
          raise InvalidQuorum, error.errmsg[/\d+/]
        when /p[rw]_val_unsatisfied/
          raise InsufficientPrimaries.new(*error.errmsg.scan(/\d+/))
        when /insufficient_vnodes/
          raise InsufficientReplicas.new(*error.errmsg.scan(/\d+/))
        when /r_val_unsatisfied/
          raise QuorumNotMet, {:r => error.errmsg.scan(/\d+/)}
        when /w_val_unsatisfied/
          vals = error.errmsg.scan(/\d+/)
          raise QuorumNotMet, {:w => [vals[0], vals[2]],
            :dw => [vals[1], vals[3]] }
        when /too_many_fails/
          raise QuorumFailed
        when /timeout/
          raise RequestTimedOut
        when /\{precommit_fail,\s*([^}]+)\}/
          raise PrecommitFailed, $1
        when /precommit_fail/
          raise PrecommitFailed
        else
          raise ServerError, error.errmsg
        end
      end
    end
  end
end
