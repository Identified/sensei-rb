module Sensei
  class Client
    def initialize optargs={}
      @query = optargs[:query]
      @facets = (optargs[:facets] || {})
      @selections = (optargs[:selections] || [])
      @other_options = optargs.dup.keep_if {|k,v| ![:query, :facets].member?(k)}
    end

    DEFAULT_FACET_OPTIONS = {:max => 6, :minCount => 1}
    
    # Add a desired facet to the results
    def facet(field, options={})
      @facets[field] = DEFAULT_FACET_OPTIONS.merge(options)
      self
    end

    def query(q)
      @query=q.to_sensei.to_h
      self
    end
    
    # Do facet selection
    def selection(field, values=[])
      if field.is_a? Hash
        @selections << field
      else
        @selections << {:terms => {field => {:values => values, :operator => "or"}}}
      end
      self
    end

    def options(opts = {})
      @other_options.merge!(opts)
      self
    end
    
    def to_h
      out = {}
      (out[:query] = @query) if @query
      (out[:facets] = @facets) if @facets.count > 0
      (out[:selections] = @selections) if @selections.count > 0
      out.merge!(@other_options)
      out
    end

    def self.construct &block
      out = self.new
      Sensei::Query.construct do
        out.instance_eval &block
      end
    end

    def run
      req = Curl::Easy.new(Webster::Config.sensei_url)
      req.http_post(self.to_h.to_json)
      JSON.parse(req.body_str)
    end
  end
end
