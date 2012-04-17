module Sensei
  class Query
    attr_accessor :options

    def &(x)
      BoolQuery.new(:operands => [self, x], :operation => :must)
    end

    def |(x)
      BoolQuery.new(:operands => [self, x], :operation => :should)
    end

    def initialize(opts={})
      @options = opts
    end

    def boost amt
      @options[:boost] = amt
    end

    def get_boost
      options[:boost] ? {:boost => options[:boost]} : {}
    end
  end

  class BoolQuery < Query
    def to_h
      {:bool => {options[:operation] => options[:operands].map(&:to_h)}}.merge(get_boost)
    end
  end

  class TermQuery < Query
    def to_h
      {:term => {options[:field] => {:value => options[:value]}.merge(get_boost)}}
    end
  end
end

class Hash
  def to_sensei
    field, value = self.first
    Sensei::Query.new(:term, :field => field, :value => value)
  end
end

