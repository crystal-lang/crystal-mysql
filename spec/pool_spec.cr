require "./spec_helper"

describe DB::Pool do
  it "should write from multiple connections" do
    channel = Channel(Nil).new
    fibers = 20
    max_pool_size = 5
    max_n = 50

    with_db "crystal_mysql_test", "max_pool_size=#{max_pool_size}" do |db|
      db.exec "create table numbers (n int, fiber int)"

      fibers.times do |f|
        spawn do
          (1..max_n).each do |n|
            db.exec "insert into numbers (n, fiber) values (?, ?)", n, f
            sleep 0.01
          end
          channel.send nil
        end
      end

      fibers.times { channel.receive }

      # all numbers were inserted
      s = fibers * max_n * (max_n + 1) // 2
      db.scalar("select sum(n) from numbers").should eq(s)

      # numbers were not inserted one fiber at a time
      rows = db.query_all "select n, fiber from numbers", as: {Int32, Int32}
      rows.map(&.[1]).should_not eq(rows.map(&.[1]).sort)
    end
  end

  it "starting multiple connections does not exceed max pool size" do
    channel = Channel(Nil).new
    fibers = 100
    max_pool_size = 5

    with_db "crystal_mysql_test", "max_pool_size=#{max_pool_size}" do |db|
      db.exec "create table numbers (n int, fiber int)"

      max_open_connections = Atomic.new(0)

      fibers.times do |f|
        spawn do
          cnn = db.checkout
          max_open_connections.max(db.pool.stats.open_connections)
          sleep 0.01
          cnn.release
          channel.send nil
        end
      end

      fibers.times { channel.receive }
      max_open_connections.get.should be <= max_pool_size
    end
  end
end
