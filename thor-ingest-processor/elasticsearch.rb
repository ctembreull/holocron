module VendorX
  module Storage
    class Elasticsearch

      attr_reader :documents, :mode

      def initialize(mode=:bulk)
        @mode      = :bulk
        @documents = []

        @config    = DaemonKit.arguments.options[:app_config]['storage']
        @config['log'] = false if @config['log'].nil?

        @client    = ::Elasticsearch::Client.new host: elasticsearch_host, log: @config['log']
      end

      #####
      # Generate a url string from configuration parts, including protocol selection and
      # inline http basic authorization. Recognizes the following configuration elements:
      # @config['proto']
      # @config['host']
      # @config['port']
      # @config['username']
      # @config['port']
      ##
      def elasticsearch_host
        host = "#{@config['host']}:#{@config['port']}"
        if (@config['username'] && @config['password'])
          host = "#{@config['username']}:#{@config['password']}@#{host}"
        end
        host = "http#{@config['proto'] == 'https' ? 's' : ''}://#{host}"

        host
      end

      def store_document(doc)
        if @mode == :bulk
          @documents << {
            index: {
              _index: @config['index'],
              _type:  @config['types']['document'],
              _id:    doc.tweet_id,
              data:   doc
            }
          }
        else
        end
      end

      def store_authors(doc, only_newer=true)
        authors = []

        if doc.is_retweet?
          authors << doc.retweet_author.merge({'updated_at' => doc.retweet_created_at})
          authors << doc.retweeted_author.merge({'updated_at' => doc.retweeted_created_at})
        else
          authors << doc.tweet_author.merge({'updated_at' => doc.tweet_created_at})
        end

        authors.each do |author|
          if only_newer
            begin
              exists = @client.get index: @config['index'], type: @config['types']['author'], id: author['screen_name']
              doc_date = Time.parse(author['updated_at'])
              old_date = Time.parse(exists['_source']['updated_at'])
              raise ::Elasticsearch::Transport::Transport::Errors::NotFound if doc_date > old_date
            rescue ::Elasticsearch::Transport::Transport::Errors::NotFound => e
              @documents << {index: {_index: @config['index'], _type: @config['types']['author'], _id: author['screen_name'], data: author}}
            end
          else
            @documents << {index: {_index: @config['index'], _type: @config['types']['author'], _id: author['screen_name'], data: author}}
          end # if only_newer
        end # authors.each
      end

      def store(flush=true)

        if DaemonKit.arguments.options[:debug]
          @documents.each do |doc|
            DaemonKit.logger.info "#{self.class.name} => store #{doc[:index][:_type]}::#{doc[:index][:_id]}"
          end
        end

        if (@mode == :bulk && @documents.length > 0)
          @client.bulk body: @documents unless DaemonKit.arguments.options[:safe]
        else
        end
        flush() if flush
      end

      def flush
        @documents = []
      end

    end
  end
end
