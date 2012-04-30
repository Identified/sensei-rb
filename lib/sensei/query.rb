# Query DSL for SenseiDB
# The basic grammar is as follows:

# query := {field => value}  (produces a term query)
#          / {field => [values ...]}  (produces a boolean query composed of 
#                                      the OR of {field => value} queries for each value)
#          / {field => (start..end)} (produces a range query on field between start and end)
#          / query & query  (ANDs two subqueries together)
#          / query | query  (ORs two subqueries together)
# 
# value := something that should probably be a string, but might work if it isn't
# 
# In theory this grammar should be literally followable (modulo operator precedence, which is
# established by Ruby), as in you should be able to arbitrarily substitute in any of the 
# productions for `query' anywhere you see a query on the RHS of a rule.
# 
# Queries can also be constructed programmatically, by instantiating the relevant query classes
# with the proper options, but that's not nearly as fun.
# 
# Note: use of the overloaded operator monkey business is best performed within the context of
# a Sensei::Query.construct block, i.e.

#      Sensei::Query.construct do
#        ({:foo => (15..30)} & {:bar => '1'}).boost!(10) | {:baz => 'wiz'}
#      end

# In particular, literal hash queries will not act as sensei queries outside of this block, but
# will instead act like whatever Arel/ActiveRecord/whatnot feels like having them be.  Thus if
# you wanted to do something similar outside of the Query.construct block, you would have to
# call #to_sensei on every hash literal instance that you see (actually just the leftmost one
# in an operator chain, but whatever).

module Sensei
  CONSTRUCT_BLOCK_KEY='in_sensei_construct'

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
      self.to_sensei.tap do |x| x.options[:boost] = amt end
    end
  end

  def self.setup_operators
    [Hash].each do |klass|
      conflicts = klass.instance_methods & Operators.instance_methods
      non_conflicts = Operators.instance_methods - conflicts

      conflicts.each do |override_method|
        klass.class_eval do
          define_method(:"#{override_method}_with_sensei_construct") do |*args|
            if Thread.current[Sensei::CONSTRUCT_BLOCK_KEY]
              self.to_sensei.send(override_method, *args)
            else
              self.send(:"#{override_method}_without_sensei_construct", *args)
            end
          end

          alias_method_chain override_method, :sensei_construct
        end
      end

      non_conflicts.each do |meth|
        klass.class_eval do
          define_method(meth.to_sym) { |*args| self.to_sensei.send(meth, *args) }
        end
      end
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

    def self.construct &block
      Thread.current[Sensei::CONSTRUCT_BLOCK_KEY] = true
      begin
        block.call
      ensure
        Thread.current[Sensei::CONSTRUCT_BLOCK_KEY] = false
      end
    end

    def not_query?
      self.is_a?(Sensei::BoolQuery) && options[:operation] == :must_not
    end

    def run(options = {})
      Sensei::Client.new(options.merge(:query => self)).search
    end
  end

  class BoolQuery < Query
    def to_h
      if self.not_query?
        raise Exception, "Error: independent boolean NOT query not allowed."
      end

      not_queries = options[:operands].select(&:not_query?).map{|x| x.options[:operands].map(&:to_h)}.flatten
      if not_queries.count > 0
        not_queries = {:must_not => not_queries}
      else
        not_queries = {}
      end

      non_not_queries = options[:operands].reject(&:not_query?)

      {:bool => {
          options[:operation] => non_not_queries.map(&:to_h)
        }.merge(get_boost).merge(not_queries)
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
