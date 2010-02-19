module Rev  
  # Implements the Memcache protocol (http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt).
  # Requires memcached >= 1.2.4 w/ noreply support
  #
  # == Usage example
  #
  #   EM.run{
  #     cache = EM::P::Memcache.connect 'localhost', 11211
  #
  #     cache.set :a, 'hello'
  #     cache.set :b, 'hi'
  #     cache.set :c, 'how are you?'
  #     cache.set :d, ''
  #
  #     cache.get(:a){ |v| p v }
  #     cache.get_hash(:a, :b, :c, :d){ |v| p v }
  #     cache.get(:a,:b,:c,:d){ |a,b,c,d| p [a,b,c,d] }
  #
  #     cache.get(:a,:z,:b,:y,:d){ |a,z,b,y,d| p [a,z,b,y,d] }
  #
  #     cache.get(:missing){ |m| p [:missing=, m] }
  #     cache.set(:missing, 'abc'){ p :stored }
  #     cache.get(:missing){ |m| p [:missing=, m] }
  #     cache.del(:missing){ p :deleted }
  #     cache.get(:missing){ |m| p [:missing=, m] }
  #   }
  #
  class Memcache < TCPSocket
    require 'rev_memcache/deferrable'
    
    include Deferrable
    ##
    # constants

    # :stopdoc:
    unless defined? Cempty
      Cstored    = 'STORED'.freeze
      Cend       = 'END'.freeze
      Cdeleted   = 'DELETED'.freeze
      Cunknown   = 'NOT_FOUND'.freeze
      Cerror     = 'ERROR'.freeze

      Cempty     = ''.freeze
      Cdelimiter = "\r\n".freeze
    end
    # :startdoc:

    ##
    # commands

    # Get the value associated with one or multiple keys
    #
    #  cache.get(:a){ |v| p v }
    #  cache.get(:a,:b,:c,:d){ |a,b,c,d| p [a,b,c,d] }
    #
    def get *keys
      raise ArgumentError unless block_given?

      callback{
        keys = keys.map{|k| k.to_s.gsub(/\s/,'_') }
        write "get #{keys.join(' ')}\r\n"
        @get_cbs << [keys, proc{ |values|
          yield *keys.map{ |k| values[k] }
        }]
      }
    end

    # Set the value for a given key
    #
    #  cache.set :a, 'hello'
    #  cache.set(:missing, 'abc'){ puts "stored the value!" }
    #
    def set key, val, exptime = 0, &cb
      callback{
        val = val.to_s
        send_cmd :set, key, 0, exptime, val.respond_to?(:bytesize) ? val.bytesize : val.size, !block_given?
        write val
        write Cdelimiter
        @set_cbs << cb if cb
      }
    end

    # Gets multiple values as a hash
    #
    #  cache.get_hash(:a, :b, :c, :d){ |h| puts h[:a] }
    #
    def get_hash *keys
      raise ArgumentError unless block_given?

      get *keys do |*values|
        yield keys.inject({}){ |hash, k| hash.update k => values[keys.index(k)] }
      end
    end

    # Delete the value associated with a key
    #
    #  cache.del :a
    #  cache.del(:b){ puts "deleted the value!" }
    #
    def delete key, expires = 0, &cb
      callback{
        write "delete #{key} #{expires}#{cb ? '' : ' noreply'}\r\n"
        @del_cbs << cb if cb
      }
    end
    alias del delete

    # :stopdoc:

    def send_cmd cmd, key, flags = 0, exptime = 0, bytes = 0, noreply = false # :nodoc:
      write "#{cmd} #{key} #{flags} #{exptime} #{bytes}#{noreply ? ' noreply' : ''}\r\n"
    end
    private :send_cmd

    ##
    # errors

    class ParserError < StandardError
    end

    ##
    # em hooks

    def on_connect
      @get_cbs = []
      @set_cbs = []
      @del_cbs = []

      @values = {}

      @reconnecting = false
      @connected = true
      succeed
      # set_delimiter "\r\n"
      # set_line_mode
    end

    def on_read data
      (@buffer||='') << data

      while index = @buffer.index(Cdelimiter)
        begin
          line = @buffer.slice!(0,index+2)
          process_cmd line
        rescue ParserError
          @buffer[0...0] = line
          break
        end
      end
    end

    def process_cmd line
      case line.strip
      when /^VALUE\s+(.+?)\s+(\d+)\s+(\d+)/ # VALUE <key> <flags> <bytes>
        bytes = Integer($3)
        # set_binary_mode bytes+2
        # @cur_key = $1
        if @buffer.size >= bytes + 2
          @values[$1] = @buffer.slice!(0,bytes)
          @buffer.slice!(0,2) # \r\n
        else
          raise ParserError
        end

      when Cend # END
        if entry = @get_cbs.shift
          keys, cb = entry
          cb.call(@values)
        end
        @values = {}

      when Cstored # STORED
        if cb = @set_cbs.shift
          cb.call(true)
        end

      when Cdeleted # DELETED
        if cb = @del_cbs.shift
          cb.call(true)
        end

      when Cunknown # NOT_FOUND
        if cb = @del_cbs.shift
          cb.call(false)
        end

      else
        p [:MEMCACHE_UNKNOWN, line]
      end
    end

    def on_close
      if @connected or @reconnecting
        # EM.add_timer(1){ reconnect @host, @port }
        @connected = false
        @reconnecting = true
        @deferred_status = nil
      else
        raise 'Unable to connect to memcached server'
      end
    end

    # :startdoc:
  end
end