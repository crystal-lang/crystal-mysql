require "./spec_helper"

def with_db(&block : DB::Database ->)
  DB.open "mysql://root@localhost", &block
end

def with_test_db(&block : DB::Database ->)
  DB.open "mysql://root@localhost" do |db|
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    db.exec "CREATE DATABASE crystal_mysql_test"
    DB.open "mysql://root@localhost/crystal_mysql_test", &block
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
  end
end

def sql(s : String)
  "#{s.inspect}"
end

def sql(s)
  "#{s}"
end

def assert_single_read(rs, value_type, value)
  rs.move_next.should be_true
  rs.read(value_type).should eq(value)
  rs.move_next.should be_false
end

def assert_single_read?(rs, value_type, value)
  rs.move_next.should be_true
  rs.read?(value_type).should eq(value)
  rs.move_next.should be_false
end

describe Driver do
  it "should register mysql name" do
    DB.driver_class("mysql").should eq(MySql::Driver)
  end

  # "SELECT 1" returns a Int64. So this test are not to be used as is on all DB::Any
  {% for value in [1_i64, "hello", 1.5] %}
    it "executes and select {{value.id}}" do
      with_db do |db|
        db.scalar("select #{sql({{value}})}").should eq({{value}})

        db.query "select #{sql({{value}})}" do |rs|
          assert_single_read rs, typeof({{value}}), {{value}}
        end
      end
    end
  {% end %}

  it "executes with bind nil" do
    with_db do |db|
      db.scalar("select ?", nil).should be_nil
    end
  end

  {% for value in [1, 1_i64, "hello", 1.5, 1.5_f32] %}
    it "executes and select nil as type of {{value.id}}" do
      with_db do |db|
        db.scalar("select null").should be_nil

        db.query "select null" do |rs|
          assert_single_read? rs, typeof({{value}}), nil
        end
      end
    end

    it "executes with bind {{value.id}}" do
      with_db do |db|
        db.scalar(%(select ?), {{value}}).should eq({{value}})
      end
    end

    it "executes with bind {{value.id}} as array" do
      with_db do |db|
        db.scalar(%(select ?), [{{value}}]).should eq({{value}})
      end
    end
  {% end %}

  it "create and drop test database" do
    sql = "SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'crystal_mysql_test'"

    with_test_db do |db|
      db.scalar(sql).should eq(1)
      db.scalar("SELECT DATABASE()").should eq("crystal_mysql_test")
    end

    with_db do |db|
      db.scalar(sql).should eq(0)
      db.scalar("SELECT DATABASE()").should be_nil
    end
  end

  it "gets column count" do
    with_test_db do |db|
      db.exec "create table person (name varchar(25), age integer)"

      db.query "select * from person" do |rs|
        rs.column_count.should eq(2)
      end
    end
  end

  it "gets column name" do
    with_test_db do |db|
      db.exec "create table person (name varchar(25), age integer)"

      db.query "select * from person" do |rs|
        rs.column_name(0).should eq("name")
        rs.column_name(1).should eq("age")
      end
    end
  end
end
