module Sensei
  class Client
    cattr_accessor :sensei_hosts, :sensei_port, :http_kafka_port, :uid_key, :http_kafka_hosts, :fake_update

    DATA_TRANSACTION_KEY = "sensei_client_data_transaction"
    TEST_TRANSACTION_KEY = "sensei_client_test_transaction"

    def self.current_data_transaction
      Thread.current[DATA_TRANSACTION_KEY].last
    end

    def self.current_test_transaction
      Thread.current[TEST_TRANSACTION_KEY].last
    end

    def self.begin_transaction key
      Thread.current[key] ||= []
      Thread.current[key] << []
    end

    def self.in_sensei_transaction? key
      Thread.current[key] ||= []
      Thread.current[key].count > 0
    end

    # This does a "data transaction," in which any update events will get
    # buffered until the block is finished, after which everything gets sent.
    def self.transaction &block
      begin
        begin_transaction DATA_TRANSACTION_KEY
        block.call
        kafka_commit(current_data_transaction)
      ensure
        Thread.current[DATA_TRANSACTION_KEY].pop
      end
    end

    def self.test_transaction &block
      begin
        begin_transaction TEST_TRANSACTION_KEY
        block.call
      ensure
        kafka_rollback(current_test_transaction)
        Thread.current[TEST_TRANSACTION_KEY].pop
      end
    end

    # Undo all of the data events that just occurred.
    # This is only really useful during tests.  Also,
    # it's only capable of rolling back insertions.
    def self.kafka_rollback(data_events)
      to_delete = data_events.select{|x| x[uid_key]}.map{|x| {:_type => '_delete', :_uid => x[uid_key]}}
      kafka_commit to_delete
    end

    def self.in_data_transaction?
      self.in_sensei_transaction? DATA_TRANSACTION_KEY
    end

    def self.in_test_transaction?
      self.in_sensei_transaction? TEST_TRANSACTION_KEY
    end

    def self.sensei_url
      raise unless sensei_hosts
      "http://#{sensei_hosts.sample}:#{sensei_port || 8080}/sensei"
    end

    def initialize optargs={}
      @query = optargs[:query].try(:to_sensei)
      @facets = (optargs[:facets] || {})
      @selections = (optargs[:selections] || {})
      @other_options = optargs.dup.keep_if {|k,v| ![:query, :facets, :selections].member?(k)}
    end

    def self.kafka_send items
      if in_data_transaction?
        current_data_transaction << items
      else
        kafka_commit items
      end

      if in_test_transaction?
        Thread.current[TEST_TRANSACTION_KEY].last << items
      end
      true
    end

    def self.kafka_commit items
      if !fake_update
        req = Curl::Easy.new("http://#{http_kafka_hosts.sample}:#{http_kafka_port}/")
        req.http_post(items.map(&:to_json).join("\n"))
        req.body_str
      end
    end

    def self.delete uids
      kafka_send(uids.map do |uid|
                   {:type => 'delete', :uid => uid.to_s}
                 end)
    end

    def self.update(documents)
      begin
        kafka_send documents
      rescue
        nil
      end
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

    def all(q)
      @query ? (@query &= q.to_sensei) : (@query = q.to_sensei)
      self
    end

    def any(q)
      @query ? (@query |= q.to_sensei) : (@query = q.to_sensei)
      self
    end

    def not(q)
      @query ? (@query &= q.to_sensei.must_not) : (@query = q.to_sensei.must_not)
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

    def self.q h
      h.to_sensei
    end

    def self.construct options={}, &block
      out = self.new(options)
      search_query = class_eval(&block)
      out.query(search_query)
    end

    def search
      req = Curl::Easy.new(self.class.sensei_url)
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
