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

  [Hash, Range].each do |klass|
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
        block.call.to_sensei
      ensure
        Thread.current[Sensei::CONSTRUCT_BLOCK_KEY] = false
      end
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
