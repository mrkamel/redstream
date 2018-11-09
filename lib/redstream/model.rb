
module Redstream
  module Model
    def self.included(base)
      base.extend(ClassMethods)
    end 

    module ClassMethods
      def redstream_callbacks(producer: Producer.new)
        after_save { |object| producer.delay object }
        after_touch { |object| producer.delay object }
        after_destroy { |object| producer.delay object }
        after_commit { |object| producer.queue object }
      end 
    end 

    def redstream_payload
      { id: id }
    end 
  end 
end

