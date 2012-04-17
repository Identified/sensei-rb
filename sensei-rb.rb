module Sensei
  module Operators
    def &(x)
      BoolQuery.new(:operands => [self.to_sensei, x.to_sensei], :operation => :must)
    end

    def |(x)
      BoolQuery.new(:operands => [self.to_sensei, x.to_sensei], :operation => :should)
    end

    def must_not
      BoolQuery.new(:operands => [self.to_sensei], :operation => :must_not)
    end

    def boost! amt
      @options[:boost] = amt
      self
    end
  end

  class Query
    attr_accessor :options

    include Operators

    def initialize(opts={})
      @options = opts
    end

    def get_boost
      options[:boost] ? {:boost => options[:boost]} : {}
    end

    def to_sensei
      self
    end
  end

  class BoolQuery < Query
    def to_h
      {:bool => {
          options[:operation] => options[:operands].map(&:to_h)
        }.merge(get_boost)
      }
    end
  end

  class TermQuery < Query
    def to_h
      {:term => {options[:field] => {:value => options[:value]}.merge(get_boost)}}
    end
  end

  class RangeQuery < Query
    def to_h
      {:range => {
          options[:field] => {
            :from => options[:from],
            :to => options[:to],
          }.merge(get_boost)
        }
      }
    end
  end
end

class Hash
  def to_sensei
    field, value = self.first
    if value.is_a? String
      Sensei::TermQuery.new(:field => field, :value => value)
    else
      value.to_sensei(field)
    end
  end
end

class Range
  def to_sensei(field)
    Sensei::RangeQuery.new(:from => self.begin, :to => self.end, :field => field)
  end
end
