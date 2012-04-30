module Sensei
  class Client
    def initialize optargs={}
      @query = optargs[:query].try(:to_sensei)
      @facets = (optargs[:facets] || {})
      @selections = (optargs[:selections] || {})
      @other_options = optargs.dup.keep_if {|k,v| ![:query, :facets, :selections].member?(k)}
    end

    DEFAULT_FACET_OPTIONS = {:max => 6, :minCount => 1}

    # Add a desired facet to the results
    def facet(field, options={})
      @facets[field] = DEFAULT_FACET_OPTIONS.merge(options)
      self
    end

    def query(q)
      @query=q.to_sensei
      self
    end

    # Do facet selection
    def selection(fields = {})
      @selections.merge!(fields)
      self
    end

    def options(opts = {})
      @other_options.merge!(opts)
      self
    end

    def to_h
      out = {}
      (out[:query] = @query.to_h) if @query
      (out[:facets] = @facets) if @facets.count > 0
      selections = @selections.map { |field, terms| {:terms => {field => {values: terms, :operator => "or"}}} }
      (out[:selections] = selections) if selections.count > 0
      out.merge!(@other_options)
      out
    end

    def self.construct &block
      out = self.new
      Sensei::Query.construct do
        out.instance_eval &block
      end
    end

    def search
      req = Curl::Easy.new(Webster::Config.sensei_url)
      req.http_post(self.to_h.to_json)
      JSON.parse(req.body_str)
    end

    # This method performs several separate queries with different
    # selection settings as necessary.  This is needed to perform
    # the common interaction pattern for faceted search, in which
    # it is desired that selections from other facets affect a
    # particular facet's counts, but a facet's own selections do
    # facet do not affect its own counts.
    def select_search
      all_selection_results = search
      facet_requests.map(&:search).each do |result|
        field, counts = result['facets'].first
        all_selection_results['facets'][field] += counts
      end

      all_selection_results['facets'] = Hash[*all_selection_results['facets'].map do |k,v|
                                               [k, v.uniq_by{|x| x['value']}]
                                             end.flatten(1)]
      all_selection_results
    end

    # This method builds the requests necessary to perform the `select_search' method.
    def facet_requests
      @selections.map do |field, values|
        Sensei::Client.new(:query => @query,
                           :facets => @facets.dup.keep_if {|name, opts| name==field},
                           :selections => @selections.dup.keep_if {|name, opts| name != field},
                           :size => 0)
      end
    end
  end
end
