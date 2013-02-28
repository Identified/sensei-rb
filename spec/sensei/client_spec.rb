require "spec_helper"

describe Sensei::Client do
  shared_examples "a configured client" do
    subject { Sensei::Client }

    describe "::configure" do
      it "sets the configs from the yaml file into its class variables" do
        subject.configure(file_path)
        expect(Sensei::Client.sensei_hosts).to match_array(["localhost"])
        expect(Sensei::Client.sensei_port).to eq(8080)
        expect(Sensei::Client.http_kafka_port).to eq(9876)
        expect(Sensei::Client.http_kafka_hosts).to match_array(["localhost"])
      end
    end
  end

  context "without RAILS_ENV" do
    context "without ERB" do
      it_behaves_like "a configured client" do
        let(:file_path) { File.dirname(__FILE__) + '/../fixtures/sensei.yml' }
      end
    end

    context "with ERB" do
      it_behaves_like "a configured client" do
        let(:file_path) {  File.dirname(__FILE__) + '/../fixtures/sensei.yml.erb' }
      end
    end

  end

 context "with RAILS_ENV" do
   before { Rails = Struct.new(:env).new("some_rails_env") }
   after { Object.send(:remove_const, :Rails) }

   context "without ERB" do
     it_behaves_like "a configured client" do
       let(:file_path) { File.dirname(__FILE__) + '/../fixtures/sensei-rails.yml' }
     end
   end

   context "with ERB" do
     it_behaves_like "a configured client" do
       let(:file_path) {  File.dirname(__FILE__) + '/../fixtures/sensei-rails.yml.erb' }
     end
   end
 end

end
