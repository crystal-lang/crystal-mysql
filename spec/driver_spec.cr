require "./spec_helper"

def with_db(&block : DB::Database ->)
  DB.open "mysql://root@localhost", &block
end

def with_test_db(options = "", &block : DB::Database ->)
  DB.open "mysql://root@localhost" do |db|
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    db.exec "CREATE DATABASE crystal_mysql_test"
    DB.open "mysql://root@localhost/crystal_mysql_test?#{options}", &block
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
  end
end

def mysql_type_for(v)
  case v
  when String ; "varchar(25)"
  when Bool   ; "bool"
  when Int8   ; "tinyint(1)"
  when Int16  ; "smallint(2)"
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

class NotSupportedType
end

describe Driver do
  it "should register mysql name" do
    DB.driver_class("mysql").should eq(MySql::Driver)
  end

  it "should connect with credentials" do
    with_db do |db|
      db.scalar("SELECT DATABASE()").should be_nil
      db.scalar("SELECT CURRENT_USER()").should match(/^root@(localhost|%)$/)

      # ensure user is deleted
      db.exec "GRANT USAGE ON *.* TO crystal_test IDENTIFIED BY 'secret'"
      db.exec "DROP USER crystal_test"
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
      db.exec "FLUSH PRIVILEGES"

      # create test db with user
      db.exec "CREATE DATABASE crystal_mysql_test"
      db.exec "CREATE USER crystal_test IDENTIFIED BY 'secret'"
      db.exec "GRANT ALL PRIVILEGES ON crystal_mysql_test.* TO crystal_test"
      db.exec "FLUSH PRIVILEGES"
    end

    DB.open "mysql://crystal_test:secret@localhost/crystal_mysql_test" do |db|
      db.scalar("SELECT DATABASE()").should eq("crystal_mysql_test")
      db.scalar("SELECT CURRENT_USER()").should match(/^crystal_test@(localhost|%)$/)
    end

    with_db do |db|
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    end
  end

  # "SELECT 1" returns a Int64. So this test are not to be used as is on all DB::Any
  {% for prepared_statements in [true, false] %}
  {% for value in [1_i64, "hello", 1.5] %}
    it "executes and select {{value.id}}" do
      with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
        db.scalar("select #{sql({{value}})}").should eq({{value}})

        db.query "select #{sql({{value}})}" do |rs|
          assert_single_read rs, typeof({{value}}), {{value}}
        end
      end
    end
  {% end %}
  {% end %}

  it "executes with bind nil" do
    with_db do |db|
      db.scalar("select ?", nil).should be_nil
    end
  end

  {% for value in [54_i16, 1_i8, 5_i8, 1, 1_i64, "hello", 1.5, 1.5_f32] %}
    {% for prepared_statements in [true, false] %}
    it "executes and select nil as type of {{value.id}}" do
      with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
        db.scalar("select null").should be_nil

        db.query "select null" do |rs|
          assert_single_read rs, typeof({{value}} || nil), nil
        end
      end
    end
    {% end %}

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

  {% for prepared_statements in [true, false] %}
  it "executes and selects blob" do
    with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
      db.exec "create table t1 (b1 BLOB)"
      db.exec "insert into t1 (b1) values (X'415A617A')"
      slice = db.scalar(%(select b1 from t1)).as(Bytes)
      slice.to_a.should eq([0x41, 0x5A, 0x61, 0x7A])
    end
  end

  types = [
    {"type" => "TINYBLOB", "size" => 10},
    {"type" => "BLOB", "size" => 1000},
    {"type" => "MEDIUMBLOB", "size" => 10000},
    {"type" => "LONGBLOB", "size" => 1000000},
  ].each do |row|
    it "set/get " + row["type"].as(String) do
      with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
        ary = UInt8[0x41, 0x5A, 0x61, 0x7A] * row["size"].as(Int32)
        slice = Bytes.new(ary.to_unsafe, ary.size)
        db.exec "create table t1 (b1 " + row["type"].as(String) + ")"
        # TODO remove when unprepared statements support args
        db.prepared.exec "insert into t1 (b1) values (?)", slice
        slice = db.scalar(%(select b1 from t1)).as(Bytes)
        slice.to_a.should eq(ary)
      end
    end
  end

  types = [
    {"type" => "TINYTEXT", "size" => 10},
    {"type" => "TEXT", "size" => 1000},
    {"type" => "MEDIUMTEXT", "size" => 10000},
    {"type" => "LONGTEXT", "size" => 100000},
  ].each do |row|
    it "set/get " + row["type"].as(String) do
      with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
        txt = "Ham Sandwich" * row["size"].as(Int32)
        db.exec "create table tab1 (txt1 " + row["type"].as(String) + ")"
        # TODO remove when unprepared statements support args
        db.prepared.exec "insert into tab1 (txt1) values (?)", txt
        text = db.scalar(%(select txt1 from tab1))
        text.should eq(txt)
      end
    end
  end

  it "gets column count" do
    with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
      db.exec "create table person (name varchar(25), age integer)"
      db.query "select * from person" do |rs|
        rs.column_count.should eq(2)
      end
    end
  end

  it "gets column name" do
    with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
      db.exec "create table person (name varchar(25), age integer)"

      db.query "select * from person" do |rs|
        rs.column_name(0).should eq("name")
        rs.column_name(1).should eq("age")
      end
    end
  end

  it "gets last insert row id" do
    with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
      db.exec "create table person (id int not null primary key auto_increment, name varchar(25), age int)"
      db.exec %(insert into person (name, age) values ("foo", 10))
      res = db.exec %(insert into person (name, age) values ("foo", 10))
      res.last_insert_id.should eq(2)
      res.rows_affected.should eq(1)
    end
  end

  {% for value in [false, true, 54_i16, 1_i8, 5_i8, 1, 1_i64, "hello", 1.5, 1.5_f32] %}
    it "insert/get value {{value.id}} from table with prepared_statements={{prepared_statements}}" do
      with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
        db.exec "create table table1 (col1 #{mysql_type_for({{value}})})"
        db.exec %(insert into table1 (col1) values (#{sql({{value}})}))

        db.query_one("select col1 from table1", as: typeof({{value}})).should eq({{value}})
      end
    end

    it "insert/get value {{value.id}} from table as nillable with prepared_statements={{prepared_statements}}" do
      with_test_db "prepared_statements=#{{{prepared_statements}}}" do |db|
        db.exec "create table table1 (col1 #{mysql_type_for({{value}})})"
        db.exec %(insert into table1 (col1) values (#{sql({{value}})}))

        db.query_one("select col1 from table1", as: typeof({{value || nil}})).should eq({{value}})
      end
    end

    it "insert/get value {{value.id}} from table with binding" do
      with_test_db do |db|
        db.exec "create table table1 (col0 varchar(25), col1 #{mysql_type_for({{value}})})"
        # the next statement will force a union in the *args
        db.exec %(insert into table1 (col0, col1) values (?, ?)), "", {{value}}
        db.query_one("select col1 from table1", as: typeof({{value}})).should eq({{value}})
      end
    end

    it "insert/get value {{value.id}} from table as nillable with binding" do
      with_test_db do |db|
        db.exec "create table table1 (col0 varchar(25), col1 #{mysql_type_for({{value}})})"
        # the next statement will force a union in the *args
        db.exec %(insert into table1 (col0, col1) values (?, ?)), "", {{value}}
        db.query_one("select col1 from table1", as: typeof({{value || nil}})).should eq({{value}})
      end
    end
  {% end %}
  {% end %}

  # zero dates http://dev.mysql.com/doc/refman/5.7/en/datetime.html - work on some mysql not others,
  # NO_ZERO_IN_DATE enabled as part of strict mode in MySQL 5.7.8. - http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-changes
  it "get datetime zero from table" do
    time1 = Time.new(0)
    with_test_db do |db|
      mode = db.scalar("SELECT @@sql_mode")
      if mode.is_a?(String) && !mode.match(/NO_ZERO_DATE/)
        db.exec "create table table1 (col1 datetime)"
        db.exec "insert into table1 (col1) values('0000-00-00 00:00:00')"
        db.scalar("select col1 from table1").should eq(time1)
      end
    end
  end

  it "get datetime null from table" do
    with_test_db do |db|
      db.exec "create table table1 (col1 datetime)"
      db.exec "insert into table1 (col1) values(null)"
      db.scalar("select col1 from table1").should eq(nil)
    end
  end

  it "get/set datetime ymd from table" do
    time1 = Time.new(2016, 2, 15)
    with_test_db do |db|
      db.exec "create table table1 (col1 datetime)"
      db.exec "insert into table1 (col1) values(?)", time1
      db.scalar("select col1 from table1").should eq(time1)
    end
  end

  it "get/set datetime ymd hms from table" do
    time1 = Time.new(2016, 2, 15, 10, 15, 30)
    with_test_db do |db|
      db.exec "create table table1 (col1 datetime)"
      db.exec "insert into table1 (col1) values(?)", time1
      db.scalar("select col1 from table1").should eq(time1)
    end
  end

  it "get/set datetime ymd hms ms from table" do
    time1 = Time.new(2016, 2, 15, 10, 15, 30, 543)
    with_test_db do |db|
      dbversion = db.scalar("SELECT VERSION();") # needs to check version, microsecond support >= 5.7
      if dbversion.is_a?(String)
        version = dbversion.match(/([0-9]+)\.([0-9]+)\.([0-9]+)/)
        if !version.nil? && version[1].to_i >= 5 && version[2].to_i >= 7
          db.exec "create table table1 (col1 datetime(3))"
          db.exec "insert into table1 (col1) values(?)", time1
          db.query("select col1 from table1") do |rs|
            rs.each do
              rs.read(Time).to_s("%Y-%m-%d %H:%M:%S.%L").should eq(time1.to_s("%Y-%m-%d %H:%M:%S.%L"))
            end
          end
        end
      end
    end
  end

  it "get timestamp from table" do
    with_test_db do |db|
      db.exec "create table table1 (m int, dt datetime, ts timestamp DEFAULT CURRENT_TIMESTAMP)"
      db.exec "insert into table1 (m, dt) values(?, NOW())", 1

      dt, ts = db.query_one "SELECT dt, ts from table1", as: {Time, Time}
      (ts - dt).total_seconds.should be_close(0.0, 0.5)
    end
  end

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

  describe "transactions" do
    it "can read inside transaction and rollback after" do
      with_test_db do |db|
        db.exec "create table person (name varchar(25))"
        db.transaction do |tx|
          tx.connection.scalar("select count(*) from person").should eq(0)
          tx.connection.exec "insert into person (name) values (?)", "John Doe"
          tx.connection.scalar("select count(*) from person").should eq(1)
          tx.rollback
        end
        db.scalar("select count(*) from person").should eq(0)
      end
    end

    it "can read inside transaction or after commit" do
      with_test_db do |db|
        db.exec "create table person (name varchar(25))"
        db.transaction do |tx|
          tx.connection.scalar("select count(*) from person").should eq(0)
          tx.connection.exec "insert into person (name) values (?)", "John Doe"
          tx.connection.scalar("select count(*) from person").should eq(1)
          # using other connection
          db.scalar("select count(*) from person").should eq(0)
        end
        db.scalar("select count(*) from person").should eq(1)
      end
    end
  end

  describe "nested transactions" do
    it "can read inside transaction and rollback after" do
      with_test_db do |db|
        db.exec "create table person (name varchar(25))"
        db.transaction do |tx_0|
          tx_0.connection.scalar("select count(*) from person").should eq(0)
          tx_0.connection.exec "insert into person (name) values (?)", "John Doe"
          tx_0.transaction do |tx_1|
            tx_1.connection.exec "insert into person (name) values (?)", "Sarah"
            tx_1.connection.scalar("select count(*) from person").should eq(2)
            tx_1.transaction do |tx_2|
              tx_2.connection.exec "insert into person (name) values (?)", "Jimmy"
              tx_2.connection.scalar("select count(*) from person").should eq(3)
              tx_2.rollback
            end
          end
          tx_0.connection.scalar("select count(*) from person").should eq(2)
          tx_0.rollback
        end
        db.scalar("select count(*) from person").should eq(0)
      end
    end
  end
end
