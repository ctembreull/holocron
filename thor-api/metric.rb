require 'esquire'
require 'pp'

module Metric

  module Core

    def client
      #conn_str = "search.thor.dev.vendorx.com:9200"
      #conn_str = "http://outpost:4LpPpwA7kBzqsk@search.thor.dev.vendorx.com:8080"
      @@api_client ||= Elasticsearch::Client.new host: ES_CONN, log: ES_CONFIG['log']
    end

    def query(q, action='search')
      client.send(action.to_sym, {index: @index, type: @type, body: q})
    end

    def MeasuredQuery(q, metric, opts={})
      opts.to_options!

      if opts[:debug] == 'json'
        puts "----------"
        pp q
        puts "----------"
      end

      result = []
      unless opts[:dry_run]
        runtime = Benchmark.realtime {
          result = self.query(q.compact!)
        }

        puts "\tQUERY: #{@project}/#{@track}/#{metric} => #{runtime * 1000} ms [#{result['took']} ms server]"
      end

      return result
    end

    def normalize_keys(buckets, old_keys, new_keys)
      old_keys.each_with_index do |k_old, index|
        k_new = new_keys[index]
        buckets.each do |h|
          h.transform_keys!{|k| k == k_old ? k_new : k}
        end
      end
      buckets
    end

    def default_interval
      15*60
    end

    def format_barcsv(docs)
      # 0 => tweet_id
      # 1 => date/time
      # 2 => username
      # 3 => user_id
      # 4 => original or retweet
      # 5 => # RTs
      # 6 => username of original author
      # 7 => # favs
      # 8 => tweet content
      separator = "|"
      processed = []

      docs.each do |doc|
        next "" unless doc['_source']

        elements = []

        fmt = Datasift.new(doc['_source'])

        elements[0] = fmt.tweet_id
        elements[1] = fmt.created_at

        if fmt.is_retweet?
          elements[2] = fmt.retweet_author_handle
          elements[3] = fmt.retweet_author_id
          elements[4] = "RT"
          elements[6] = fmt.retweeted_author_handle
        else
          elements[2] = fmt.tweet_author_name
          elements[3] = fmt.tweet_author_id
          elements[4] = "O"
          elements[6] = "NONE"
        end
        elements[5] = 0
        elements[7] = 0
        elements[8] = fmt.content

        processed << elements.join(separator)
      end

      processed
    end

  end

  class TopAuthors
    include Core

    @@client = nil

    attr_accessor :project, :track, :type
    def initialize(project, track=nil)
      @index   = 'thor'
      @type    = 'tweet'
      @project = project.downcase.gsub(/\s+/,'_')
      @track   = track
      @type    = 'tweet'
    end

    ##
    # DATASET: highest-volume authors
    # SCOPE:   ALL documents in project/tracking/time plane
    #
    def by_volume(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.aggregations[:top_authors] = {terms: {field: 'interaction.author.username', size: 50}}

      result = self.MeasuredQuery(q.build, "authors.#{__method__}", opts)
      unless result.nil? || result.empty?
        list = result['aggregations']['top_authors']['buckets']
        list = self.normalize_keys(list, ['key', 'doc_count'], ['screen_name', 'count'])
        return list
      end

      return result
    end

    ##
    # DATASET: most commonly mentioned users
    # SCOPE:   all documents in project/tracking/time plane
    #
    def by_mention(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])

      q.facets[:top_authors] = {terms: {field: 'interaction.mentions', size: 50, shard_size: 50}}

      result = self.MeasuredQuery(q.build, "authors.#{__method__}", opts)
      unless result.nil? || result.empty?
        list = result['facets']['top_authors']['terms']
        list = self.normalize_keys(list, ['term'], ['screen_name'])
        list
      end
    end

    ##
    # DATASET: highest-volume authors
    # SCOPE:   ONLY documents containing TOP HASHTAGS in project/tracking/time plane
    #
    def by_top_hashtags(opts={})
      opts.to_options!

      hashtag_search = Metric::TopHashtags.new(@project, @track)
      top_hashtags   = hashtag_search.by_volume(opts).map{|bucket| bucket['hashtag']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {terms: {'interaction.hashtags' => top_hashtags}}
      q.facets[:top_authors] = {terms: {field: 'interaction.author.username', size: 50, shard_size: 50}}

      result = self.MeasuredQuery(q.build, "authors.#{__method__}", opts)
      unless result.nil? || result.empty?
        list = result['facets']['top_authors']['terms']
        list = self.normalize_keys(list, ['term'], ['screen_name'])
        list
      end
    end

    ##
    # DATASET: most frequently-mentioned authors
    # SCOPE:   ONLY documents authored by TOP AUTHORS in project/tracking/time plane
    #
    def mentioned_by_top_authors(opts={})
      opts.to_options!

      top_authors = self.by_volume(opts).map{|bucket| bucket['screen_name']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {terms: {'interaction.author.username' => top_authors}}
      q.facets[:top_authors] = {terms: {field: 'interaction.mentions', size: 50, shard_size: 50}}

      result = self.MeasuredQuery(q.build, "authors.#{__method__}", opts)
      unless result.nil? || result.empty?
        list = result['facets']['top_authors']['terms']
        list = self.normalize_keys(list, ['term'], ['screen_name'])
        list
      end
    end
  end

  class TopHashtags
    include Core

    attr_accessor :project, :track, :type
    def initialize(project, track=nil)
      @index   = 'thor'
      @project = project.downcase.gsub(/\s+/, '_')
      @track   = track
      @type    = 'tweet'
    end

    ##
    # DATASET: highest-volume hashtags
    # SCOPE:   ALL documents in project/tracking/time plane
    #
    def by_volume(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {exists: {field: 'interaction.hashtags'}}
      q.facets[:top_hashtags] = {terms: {field: 'interaction.hashtags', size: 50, shard_size: 50}}

      result = self.MeasuredQuery(q.build, "hashtags.#{__method__}", opts)
      unless result.nil? || result.empty?
        list = result['facets']['top_hashtags']['terms']
        list = self.normalize_keys(list, ['term'], ['hashtag'])
        list
      end
    end

    ##
    # DATASET: highest-volume hashtags
    # SCOPE:   ONLY documents authored by TOP AUTHORS in project/tracking/time plane WHICH CONTAIN hashtags
    #
    def by_top_authors(opts={})
      opts.to_options!

      author_search = Metric::TopAuthors.new(@project, @track)
      top_authors   = author_search.by_volume(opts).map{|bucket| bucket['screen_name']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {exists: {field: 'interaction.hashtags'}}
      q.qfilters << {terms: {'interaction.author.username' => top_authors}}
      q.facets[:top_hashtags] = {terms: {field: 'interaction.hashtags', size: 50, shard_size: 50}}

      result = self.MeasuredQuery(q.build, "hashtags.#{__method__}", opts)
      unless result.nil? || result.empty?
        list = result['facets']['top_hashtags']['terms']
        list = self.normalize_keys(list, ['term'], ['hashtag'])
        list
      end
    end
  end

  class AuthorMetrics
    include Core

    attr_accessor :author
    def initialize
      @index = 'thor'
      @type  = 'author'
    end

    def fetch_list(authors=[])
      authors = authors.map(&:downcase)
      q = {size: authors.size, query: {terms: {screen_name: authors}}}

      result = self.MeasuredQuery(q, {})
      unless result.nil? || result.empty?
        list = result['hits']['hits'].map{|hit| hit['_source']}
        list
      end
    end

    def fetch_one(author)
      q = {size: 1, query: {match: {screen_name: author}}}

      result = self.MeasuredQuery(q, {})
      if result['hits']['hits'].size > 0
        result['hits']['hits'][0]['_source']
      else
        {}
      end
    end

    def by_volume(opts={})
      authors_search = Metric::TopAuthors.new(@project, @track)
      top_authors    = authors_search.by_volume(opts).map{|bucket| bucket['screen_name'].downcase}

      q = {query: {terms: {screen_name: top_authors}}}

      result = self.MeasuredQuery(q, opts)
      unless result.nil? || result.empty?
        list = result['hits']['hits'].map{|hit| hit['_source']}
        list
      end
    end

    def by_mention(opts={})
      authors_search = Elastic::TopAuthors.new(@project, @track)
      top_authors    = authors_search.by_mention(opts).map{|bucket| bucket['screen_name']}

      q = {query: {terms: {screen_name: top_authors}}}

      result = self.MeasuredQuery(q, opts)
      unless result.nil? || result.empty?
        list = result['hits']['hits'].map{|hit| hit['_source']}
        list
      end
    end

    def by_top_hashtags(opts={})
      authors_search = Elastic::TopAuthors.new(@project, @track)
      top_authors    = authors_search.by_top_hashtags(opts).map{|bucket| bucket['screen_name']}

      q = {query: {terms: {screen_name: top_authors}}}

      result = self.MeasuredQuery(q, opts)
      unless result.nil? || result.empty?
        list = result['hits']['hits'].map{|hit| hit['_source']}
        list
      end
    end

    def mentioned_by_top_authors(opts={})
      authors_search = Metric::TopAuthors.new(@project, @track)
      top_authors    = authors_search.mentioned_by_top_authors(opts).map{|bucket| bucket['screen_name']}

      q = {query: {terms: {screen_name: top_authors}}}

      result = self.MeasuredQuery(q, opts)
      unless result.nil? || result.empty?
        list = result['hits']['hits'].map{|hit| hit['_source']}
        list
      end
    end
  end

  class HashtagTweets
    include Core
    attr_accessor :hashtag, :project, :track, :type
    def initialize(hashtag, project, track=nil)
      @index   = 'thor'
      @project = project.downcase.gsub(/\s+/,'_')
      @track   = track
      @hashtag = hashtag
      @type    = 'tweet'
    end

    def by_volume(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qquery = {term: {'interaction.hashtags' => @hashtag}}

      result = self.MeasuredQuery(q.build, "hashtag.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def by_top_authors(opts={})
      opts.to_options!

      authors_search = Metric::TopAuthors.new(@project, @track)
      top_authors    = authors_search.by_volume(opts).map{|bucket| bucket['screen_name']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {terms: {'interaction.author.username' => top_authors}}
      q.qquery = {term: {'interaction.hashtags' => @hashtag}}

      result = self.MeasuredQuery(q.build, "hashtag.tweets.#{__method__}", opts)
      result['hits']['hits']
    end
  end

  class AuthorTweets
    include Core

    attr_accessor :author, :project, :track, :type
    def initialize(author, project, track=nil)
      @index   = 'thor'
      @project = project.downcase.gsub(/\s+/,'_')
      @track   = track
      @author  = author
      @type    = 'tweet'
    end

    def by_volume(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qquery = {match: {'interaction.author.username' => @author}}

      result = self.MeasuredQuery(q.build, "author.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def by_mention(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qquery = {term: {'interaction.mentions' => @author}}

      result = self.MeasuredQuery(q.build, "author.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def by_top_hashtags(opts={})
      opts.to_options!

      hashtags_search = Metric::TopHashtags.new(@project, @track)
      top_hashtags    = hashtags_search.by_volume(opts).map{|bucket| bucket['hashtag']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {terms: {'interaction.hashtags' => top_hashtags}}
      q.qquery = {match: {'interaction.author.username' => @author}}

      result = self.MeasuredQuery(q.build, "author.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def mentioned_by_top_authors(opts={})
      opts.to_options!

      authors_search = Metric::TopAuthors.new(@project, @track)
      top_authors    = authors_search.by_volume(opts).map{|bucket| bucket['screen_name']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {terms: {'interaction.author.username' => top_authors}}
      q.qquery = {term: {'interaction.mentions' => @author}}

      result = self.MeasuredQuery(q.build, "author.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def top_mentioner(opts={})
      opts.to_options!

      authors_search = Metric::TopAuthors.new(@project, @track)
      top_authors    = authors_search.by_volume(opts).map{|bucket| bucket['screen_name']}

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {terms: {'interaction.author.username' => top_authors}}
      q.qquery = {term: {'interaction.mentions' => @author}}
      q.facets[:top_mentioning_authors] = {terms: {field: 'interaction.author.username', size: 1}}

      result = self.MeasuredQuery(q.build, "author.tweets.#{__method__}", opts)
    end
  end

  class ProjectTweets
    include Core
    attr_accessor :project, :track, :type
    def initialize(project, track=nil)
      @index   = 'thor'
      @project = project.downcase.gsub(/\s+/, '_')
      @track   = track
      @type    = 'tweet'
    end

    def all(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])

      result = self.MeasuredQuery(q.build, "project.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def tweets(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {exists: {field: "twitter.user"}}

      result = self.MeasuredQuery(q.build, "project.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def replies(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {exists: {field: "twitter.in_reply_to_user_id"}}

      result = self.MeasuredQuery(q.build, "project.tweets.#{__method__}", opts)
      result['hits']['hits']
    end

    def retweets(opts={})
      opts.to_options!

      q = Esquire.new
      q.configure(opts)
      q.source_filter(@project, @track)
      q.date_filter(opts[:start_date], opts[:end_date])
      q.qfilters << {exists: {field: "twitter.retweet"}}

      result = self.MeasuredQuery(q.build, "project.tweets.#{__method__}", opts)
      result['hits']['hits']
    end
  end

  class AuthorSnapshotData
    include Core

    attr_accessor :project, :track, :type
    def initialize(author, project, track=nil)
      @index   = 'thor'
      @project = project.downcase.gsub(/\s+/,'_')
      @track   = track
      @author  = author
      @type    = 'tweet'
    end

    def tweets(opts={})
      opts.to_options!
      opts[:interval] = default_interval if opts[:interval].nil?

      q = Esquire.new
      q.configure(opts)
      q.qquery = {match: {'interaction.author.username' => @author}}
      q.facets[:tweets_over_time] = {
        histogram: {
          key_field: 'interaction.created_at',
          time_interval: opts[:interval] * 1000
        }
      }

      result = self.MeasuredQuery(q.build, "author.snapshot.tweets", opts)
      unless result.nil? || result.empty?
        list = result['facets']['tweets_over_time']['entries']
        list
      end
    end

    def retweets(opts={})
      opts.to_options!
      opts[:interval] = default_interval if opts[:interval].nil?

      q = Esquire.new
      q.configure(opts)
      q.qquery = {match: {'interaction.author.username' => @author}}
      q.facets[:retweets_over_time] = {
        histogram: {
          key_field: 'twitter.retweet.created_at',
          time_interval: opts[:interval] * 1000
        }
      }

      result = self.MeasuredQuery(q.build, "author.snapshot.retweets", opts)
      unless result.nil? || result.empty?
        list = result['facets']['retweets_over_time']['entries']
        list
      end
    end

    def followers(opts={})
      opts.to_options!
      opts[:interval] = default_interval if opts[:interval].nil?

      q = Esquire.new
      q.configure(opts)
      q.qquery = {match: {'interaction.author.username' => @author}}
      q.facets[:followers_over_time] = {
        histogram: {
          key_field: 'interaction.created_at',
          value_script: 'author_followers',
          time_interval: opts[:interval] * 1000
        }
      }

      result = self.MeasuredQuery(q.build, "author.snapshot.followers", opts)
      unless result.nil? || result.empty?
        list = result['facets']['followers_over_time']['entries']
        list
      end
    end

    def following(opts={})
      opts.to_options!
      opts[:interval] = default_interval if opts[:interval].nil?

      q = Esquire.new
      q.configure(opts)
      q.qquery = {match: {'interaction.author.username' => @author}}
      q.facets[:following_over_time] = {
        histogram: {
          key_field: 'interaction.created_at',
          value_script: 'author_following',
          time_interval: opts[:interval] * 1000
        }
      }


      result = self.MeasuredQuery(q.build, "author.snapshot.following", opts)
      unless result.nil? || result.empty?
        list = result['facets']['following_over_time']['entries']
        list
      end
    end

    def favorites(opts={})
      opts.to_options!
      opts[:interval] = default_interval if opts[:interval].nil?

      q = Esquire.new
      q.configure(opts)
      q.qquery = {match: {'interaction.author.username' => @author}}
      q.facets[:favorites_over_time] = {
        histogram: {
          key_field: 'interaction.created_at',
          value_script: 'author_favorites',
          time_interval: opts[:interval] * 1000
        }
      }

      result = self.MeasuredQuery(q.build, "author.snapshot.favorites", opts)
      unless result.nil? || result.empty?
        list = result['facets']['favorites_over_time']['entries']
        list
      end
    end
  end

  class Report
    attr_accessor :project, :sources, :start_date, :end_date, :limit, :excluded_authors, :excluded_hashtags, :excluded_topics

    def initialize(project, start_date=0, end_date=0)
      @index   = 'thor'
      @project = Project.find_by(tag: project)
      @sources = {
        'All'     => %w[ TopAuthors TopAuthorHashtags TopMentions TopMentionAuthors TopHashtagAuthors TopHashtags ],
        'KFollow' => %w[ TopAuthors TopAuthorHashtags TopHashtags TopHashtagAuthors TopMentions TopMentionAuthors ],
        'AFollow' => %w[ TopAuthors TopAuthorHashtags TopMentions TopMentionAuthors ],
        'HFollow' => %w[ TopHashtags TopHashtagAuthors TopMentions TopMentionAuthors ]
      }
      @start_date = start_date
      @end_date   = end_date
      @limit      = 50
    end

    def build_report
      responses = {}

      opts                     = {from: 0}
      opts[:start_date]        = @start_date
      opts[:end_date]          = @end_date
      opts[:excluded_authors]  = @project.excluded_authors
      opts[:excluded_hashtags] = @project.excluded_hashtags
      opts[:excluded_topics]   = @project.excluded_topics

      runtime = Benchmark.realtime {
        @sources.each_pair do |source, metrics|
          puts "Source: #{source}"
          responses[source] = {}

          source_key = case source
            when 'AFollow' then 'authors'
            when 'HFollow' then 'hashtags'
            when 'KFollow' then 'keywords'
            else nil
          end
          top_authors  = Metric::TopAuthors.new(@project.tag, source_key)
          top_hashtags = Metric::TopHashtags.new(@project.tag, source_key)

          metrics.each do |metric|
            puts "\tMetric: #{metric}"
            opts[:size] = (metric == 'TopHashtags' || metric== 'TopAuthorHashtags') ? 20 : 50

            begin
              resp = case metric
                when 'TopAuthors'        then top_authors.by_volume(opts)
                when 'TopMentions'       then top_authors.by_mention(opts)
                when 'TopHashtagAuthors' then top_authors.by_top_hashtags(opts)
                when 'TopMentionAuthors' then top_authors.mentioned_by_top_authors(opts)
                when 'TopHashtags'       then top_hashtags.by_volume(opts)
                when 'TopAuthorHashtags' then top_hashtags.by_top_authors(opts)
                else []
              end

              data = []
              resp.each do |row|
                rhash = row.to_hash
                if metric == 'TopMentionAuthors'
                  #es = Metric::AuthorTweets.new(rhash['screen_name'], @project.tag, nil)
                  rhash['mention_author_screenname'] = rhash['screen_name']
                  rhash['author_screenname'] = ''
                  #rhash['author_screenname']         = es.top_mentioner(opts)
                end
                data << rhash
              end

              responses[source][metric] = (data.sort_by{|o| o['count']}).reverse
            rescue RuntimeError => e
              responses[source][metric] = []
            end # begin | rescue | end
          end # metrics.each
        end # @sources.each_pair
      } # Benchmark.realtime

      puts "Report Time taken: #{runtime * 1000} milliseconds"
      puts "---------------------------------------"

      responses

    end # def build_report
  end # class Report

end




class Hash
  def symbolize_keys!
    transform_keys!{ |key| key.to_sym rescue key }
  end
  alias_method :to_options!, :symbolize_keys!

  def transform_keys!
    keys.each do |key|
      self[yield(key)] = delete(key)
    end
    self
  end

  def compact!
    self.delete_if{|k,v| v.nil? || (v.is_a? Hash and v.empty?)}
    self
  end
end
