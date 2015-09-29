
class Esquire
  attr_accessor :qquery, :qfilters, :facets, :aggregations

  def initialize(size=50, from=nil)
    @qquery       = nil
    @qfilters     = []
    @nfilters     = []
    @aggregations = {}
    @facets       = {}
    @size         = size
    @from         = from
  end

  def configure(opts={})
    @size = opts[:size] unless opts[:size].nil?
    @from = opts[:from] unless opts[:from].nil?

    unless opts[:excluded_authors].nil?
      @nfilters << {terms: {'interaction.author.username' => opts[:excluded_authors]}}
      @nfilters << {terms: {'interaction.mentions' => opts[:excluded_authors]}} unless opts[:exclude_author_mentions].nil?
    end
    @nfilters << {terms: {'interaction.hashtags' => opts[:excluded_hashtags]}} unless opts[:excluded_hashtags].nil?
    unless opts[:excluded_topics].nil?
      opts[:excluded_topics].each do |xtopic|
        @nfilters << {exists: {field: xtopic}}
      end
    end

    unless @nfilters.empty?
      @qfilters << {not: {filter: {or: @nfilters}}}
    end
  end

  def base_query
    {sort: [{'interaction.created_at' => {order: 'desc'}},{'interaction.author.username' => {order: 'asc'}}], size: @size}
  end

  def build
    _q = base_query
    if @qquery.nil?
      _q.merge!(filtered_query) unless @qfilters.empty?
    else
      if @qfilters.empty?
        _q[:query] = @qquery
      else
        _q.merge!(filtered_query)
      end
    end

    _q[:aggs]   = @aggregations unless @aggregations.empty?
    _q[:facets] = @facets unless @facets.empty?
    _q[:size]   = @size unless @size.nil?
    _q[:from]   = @from unless @from.nil?

    _q
  end

  def to_json
    build.to_json
  end

  def filtered_query
    _q = {query: {filtered: {filter: {}}}}
    _q[:query][:filtered][:query] = @qquery unless @qquery.nil?
    if @qfilters.size > 1
      _and = []
      @qfilters.each{|qf| _and << qf}
      _q[:query][:filtered][:filter][:and] = _and
    else
      _q[:query][:filtered][:filter] = @qfilters.first
    end
    _q
  end

  def source_filter(project=nil, track=nil)
    return if project.nil?
    if track.nil?
      @qfilters << {exists: {field: "interaction.tag_tree.#{project}"}}
    else
      @qfilters << {query: {multi_match: {query: track, fields: "interaction.tag_tree.#{project}.*"}}}
    end
  end

  # start_date and end_date are integers. We don't know the times they represent, though.
  def date_filter(start_date, end_date=nil)
    sdate = Time.at(start_date).utc.beginning_of_day.to_i * 1000
    edate = Time.at(end_date).utc.end_of_day.to_i * 1000

    @qfilters << {range: {'interaction.created_at' => {gte: sdate, lte: edate}}}
  end

  def and_filter(filter)
    return filter if filter.keys.size <= 1

    new_filter = {and: []}
    filter.each{|k,v| new_filter[:and] << {k => v}}
    new_filter
  end
end
