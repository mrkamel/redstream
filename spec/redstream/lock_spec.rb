
require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Lock do
  describe "#acquire" do
    it "gets a lock" do
      lock_results = Concurrent::Array.new
      calls = Concurrent::AtomicFixnum.new(0)

      threads = Array.new(2) do |i|
        Thread.new do
          lock_results << Redstream::Lock.new(name: "lock").acquire do
            calls.increment

            sleep 1
          end
        end
      end

      threads.each(&:join)

      expect(calls.value).to eq(1)
      expect(lock_results.to_set).to eq([1, nil].to_set)
    end

    it "keeps the lock" do
      threads = []
      calls = Concurrent::Array.new

      threads << Thread.new do
        Redstream::Lock.new(name: "lock").acquire do
          calls << "thread-1"

          sleep 6
        end
      end

      sleep 6

      threads << Thread.new do
        Redstream::Lock.new(name: "lock").acquire do
          calls << "thread-2"
        end
      end

      threads.each(&:join)

      expect(calls).to eq(["thread-1"])
    end

    it "does not lock itself" do
      lock = Redstream::Lock.new(name: "lock")

      lock_results = []
      calls = 0

      2.times do
        lock_results << lock.acquire do
          calls += 1
        end
      end

      expect(calls).to eq(2)
      expect(lock_results).to eq([1, 1])
    end
  end
end

