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

def mysql_type_for(v)
  case v
  when String ; "varchar(25)"
  when Int32  ; "int"
  when Int64  ; "bigint"
  when Float32; "float"
  when Float64; "double"
  else
    raise "not implemented for #{typeof(v)}"
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

class NotSupportedType
end

describe Driver do
  it "should register mysql name" do
    DB.driver_class("mysql").should eq(MySql::Driver)
  end

  it "should connect with credentials" do
    with_db do |db|
      db.scalar("SELECT DATABASE()").should be_nil
      db.scalar("SELECT CURRENT_USER()").should eq("root@localhost")

      # ensure user is deleted
      db.exec "GRANT USAGE ON *.* TO crystal_test@localhost IDENTIFIED BY 'secret'"
      db.exec "DROP USER crystal_test@localhost"
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
      db.exec "FLUSH PRIVILEGES"

      # create test db with user
      db.exec "CREATE DATABASE crystal_mysql_test"
      db.exec "CREATE USER crystal_test@localhost IDENTIFIED BY 'secret'"
      db.exec "GRANT ALL PRIVILEGES ON crystal_mysql_test.* TO crystal_test@localhost"
      db.exec "FLUSH PRIVILEGES"
    end

    DB.open "mysql://crystal_test:secret@localhost/crystal_mysql_test" do |db|
      db.scalar("SELECT DATABASE()").should eq("crystal_mysql_test")
      db.scalar("SELECT CURRENT_USER()").should eq("crystal_test@localhost")
    end

    with_db do |db|
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    end
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

  it "executes and selects blob" do
    with_test_db do |db|
      db.exec "create table t1 (b1 BLOB)"
      db.exec "insert into t1 (b1) values (X'415A617A')"
      slice = db.scalar(%(select b1 from t1)).as(Bytes)
      slice.to_a.should eq([0x41, 0x5A, 0x61, 0x7A])
    end
  end

  it "executes with bind blob" do
    with_test_db do |db|
      ary = UInt8[0x41, 0x5A, 0x61, 0x7A]
      slice = Bytes.new(ary.to_unsafe, ary.size)

      db.exec "create table t1 (b1 BLOB)"
      db.exec "insert into t1 (b1) values (?)", slice

      slice = db.scalar(%(select b1 from t1)).as(Bytes)
      slice.to_a.should eq(ary)
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

  it "gets column types" do
    with_test_db do |db|
      db.exec "create table table1 (aText varchar(25), anInteger int, anBinInteger bigint, aFloat float, aDouble double, aBlob blob)"

      db.query "select * from table1" do |rs|
        rs.column_type(0).should eq(String)
        rs.column_type(1).should eq(Int32)
        rs.column_type(2).should eq(Int64)
        rs.column_type(3).should eq(Float32)
        rs.column_type(4).should eq(Float64)
        rs.column_type(5).should eq(Bytes)
      end
    end
  end

  it "gets last insert row id" do
    with_test_db do |db|
      db.exec "create table person (id int not null primary key auto_increment, name varchar(25), age int)"

      db.exec %(insert into person (name, age) values ("foo", 10))

      res = db.exec %(insert into person (name, age) values ("foo", 10))
      res.last_insert_id.should eq(2)
      res.rows_affected.should eq(1)
    end
  end

  {% for value in [1, 1_i64, "hello", 1.5, 1.5_f32] %}
    it "insert/get value {{value.id}} from table" do
      with_test_db do |db|
        db.exec "create table table1 (col1 #{mysql_type_for({{value}})})"
        db.exec %(insert into table1 (col1) values (#{sql({{value}})}))
        db.scalar("select col1 from table1").should eq({{value}})
      end
    end
  {% end %}

  it "raises on unsupported param types" do
    with_db do |db|
      expect_raises Exception, "MySql::Type does not support NotSupportedType values" do
        db.query "select ?", NotSupportedType.new
      end
      # TODO raising exception does not close the connection and pool is exhausted
    end

    with_db do |db|
      expect_raises Exception, "MySql::Type does not support NotSupportedType values" do
        db.exec "select ?", NotSupportedType.new
      end
    end
  end

  it "gets many rows from table" do
    with_test_db do |db|
      db.exec "create table person (name varchar(25), age int)"
      db.exec %(insert into person values ("foo", 10))
      db.exec %(insert into person values ("bar", 20))
      db.exec %(insert into person values ("baz", 30))

      names = [] of String
      ages = [] of Int32
      db.query "select * from person" do |rs|
        rs.each do
          names << rs.read(String)
          ages << rs.read(Int32)
        end
      end
      names.should eq(["foo", "bar", "baz"])
      ages.should eq([10, 20, 30])
    end
  end

  it "ensures statements are closed" do
    with_test_db do |db|
      DB.open "mysql://root@localhost/crystal_mysql_test" do |db|
        db.exec %(create table if not exists a (i int not null, str text not null);)
        db.exec %(insert into a (i, str) values (23, "bai bai");)
      end

      2.times do |i|
        DB.open "mysql://root@localhost/crystal_mysql_test" do |db|
          begin
            db.query("SELECT i, str FROM a WHERE i = ?", 23) do |rs|
              rs.move_next
              break
            end
          rescue e
            fail("Expected no exception, but got \"#{e.message}\"")
          end

          begin
            db.exec("UPDATE a SET i = ? WHERE i = ?", 23, 23)
          rescue e
            fail("Expected no exception, but got \"#{e.message}\"")
          end
        end
      end
    end
  end
end
