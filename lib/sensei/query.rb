# Query DSL for SenseiDB
# The basic grammar is as follows:

# query :=   q(field => value)  (produces a term query)
#          / q(field => [values ...])  (produces a boolean query composed of 
#                                      the OR of {field => value} queries for each value)
#          / q(field => (start..end)) (produces a range query on field between start and end)
#          / query & query  (ANDs two subqueries together)
#          / query | query  (ORs two subqueries together)
# 
# value := something that should probably be a string, but might work if it isn't
# 
# Note: use of the `q' operator must be performed within the context of
# a Sensei::Query.construct block, i.e.

#      Sensei::Query.construct do
#        (q(:foo => (15..30)) & q(:bar => '1')).boost!(10) | q(:baz => 'wiz')
#      end

# If you're not in a construct block, you can still do Sensei::Query.q(...).

module Sensei
  module Operators
    def &(x)
      BoolQuery.new(:operands => [self.to_sensei, x.to_sensei], :operation => :must)
    end

    def |(x)
      BoolQuery.new(:operands => [self.to_sensei, x.to_sensei], :operation => :should)
    end

    def ~
      self.must_not
    end

    def must_not
      BoolQuery.new(:operands => [self.to_sensei], :operation => :must_not)
    end

    def boost! amt
      self.to_sensei.tap do |x| x.options[:boost] = amt end
    end
  end

  class Query
    attr_accessor :options
    cattr_accessor :result_klass

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

    def self.construct &block
      class_eval(&block)
    end

    def self.q(h)
      h.to_sensei
    end

    def not_query?
      self.is_a?(Sensei::BoolQuery) && options[:operation] == :must_not
    end

    def run(options = {})
      results = Sensei::Client.new(options.merge(:query => self)).search
      if @@result_klass
        @@result_klass.new(results)
      else
        results
      end
    end
  end

  class BoolQuery < Query
    def operands
      options[:operands]
    end

    def to_h
      if self.not_query?
        raise Exception, "Error: independent boolean NOT query not allowed."
      end

      not_queries, non_not_queries = operands.partition(&:not_query?)
      not_queries = not_queries.map{|x| x.operands.map(&:to_h)}.flatten

      non_not_queries = non_not_queries.reject{|x| x.is_a? AllQuery} if options[:operation] == :must

      subqueries = non_not_queries.map(&:to_h)
      mergeable, nonmergeable = subqueries.partition do |x|
        isbool = x[:bool]
        sameop = isbool && isbool[options[:operation]]
        boosted = isbool && isbool[:boost]
        isbool && sameop && (boosted.nil? || boosted == options[:boost])
      end
      merged_queries = mergeable.map{|x| x[:bool][options[:operation]]}.flatten(1)
      merged_nots = mergeable.map{|x| x[:bool][:must_not] || []}.flatten(1)

      all_nots = merged_nots + not_queries
      not_clause = (all_nots.count > 0 ? {:must_not => all_nots} : {})

      {:bool => {
          options[:operation] => nonmergeable + merged_queries
        }.merge(get_boost).merge(not_clause)
      }
    end
  end

  class TermQuery < Query
    def to_h
      {:term => {options[:field] => {:value => options[:value].to_s}.merge(get_boost)}}
    end
  end

  class RangeQuery < Query
    def to_h
      {:range => {
          options[:field] => {
            :from => options[:from],
            :to => options[:to],
            :_type => (options[:from].is_a?(Float) || options[:to].is_a?(Float)) ? "double" : "float"
          }.merge(get_boost)
        },
      }
    end
  end

  class AllQuery < Query
    def to_h
      {:match_all => {}.merge(get_boost)}
    end
  end
end

class Hash
  def to_sensei
    field, value = self.first
    if [String, Fixnum, Float, Bignum].member?(value.class)
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

class Array
  def to_sensei(field, op=:should)
    Sensei::BoolQuery.new(:operation => op, :operands => self.map{|value| {field => value}.to_sensei})
  end
end
