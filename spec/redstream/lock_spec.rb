require File.expand_path("../spec_helper", __dir__)

RSpec.describe Redstream::Lock do
  describe "#acquire" do
    it "gets a lock" do
      lock_results = Concurrent::Array.new
      calls = Concurrent::AtomicFixnum.new(0)

      threads = Array.new(2) do |_i|
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

      sleep 5

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

    it "releases the lock and notifies" do
      lock = Redstream::Lock.new(name: "lock")

      expect(redis.llen("#{Redstream.lock_key_name("lock")}.notify")).to eq(0)

      lock.acquire do
        # nothing
      end

      expect(redis.exists?(Redstream.lock_key_name("lock"))).to eq(false)
      expect(redis.llen("#{Redstream.lock_key_name("lock")}.notify")).to eq(1)
    end

    it "does not release the lock when the lock is already taken again" do
      lock = Redstream::Lock.new(name: "lock")

      lock.acquire do
        redis.set(Redstream.lock_key_name("lock"), "other")
      end

      expect(redis.get(Redstream.lock_key_name("lock"))).to eq("other")
    end

    it "acquires the lock as soon as it gets released" do
      time = nil

      thread = Thread.new do
        Redstream::Lock.new(name: "lock").acquire do
          time = Time.now.to_f

          sleep 2
        end
      end

      sleep 1

      lock = Redstream::Lock.new(name: "lock")
      lock.wait(10) until lock.acquire { "nothing" }

      thread.join

      expect(Time.now.to_f - time).to be < 3
    end
  end

  describe "#wait" do
    it "blocks for the specified time max" do
      stopped = false

      thread = Thread.new do
        Redstream::Lock.new(name: "lock").acquire do
          sleep 0.1 until stopped
        end
      end

      time = Time.now.to_f

      Redstream::Lock.new(name: "lock").wait(2)

      expect(Time.now.to_f - time).to be < 3

      stopped = true

      thread.join
    end

    it "wakes up when the lock gets released" do
      thread = Thread.new do
        Redstream::Lock.new(name: "lock").acquire do
          sleep 2
        end
      end

      time = Time.now.to_f

      Redstream::Lock.new(name: "lock").wait(10)

      expect(Time.now.to_f - time).to be < 3

      thread.join
    end
  end
end
