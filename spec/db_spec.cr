require "./spec_helper"
require "db/spec"
require "semantic_version"

private class NotSupportedType
end

DB::DriverSpecs(MySql::Any).run do
  before do
    DB.open db_url do |db|
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
      db.exec "CREATE DATABASE crystal_mysql_test"
    end
  end
  after do
    DB.open db_url do |db|
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    end
  end

  connection_string db_url("crystal_mysql_test")

  sample_value true, "bool", "true", type_safe_value: false
  sample_value false, "bool", "false", type_safe_value: false
  sample_value 5_i8, "tinyint(1)", "5", type_safe_value: false
  sample_value 54_i16, "smallint(2)", "54", type_safe_value: false
  sample_value 123, "mediumint(2)", "123", type_safe_value: false
  sample_value 1, "int", "1", type_safe_value: false
  sample_value 1_i64, "bigint", "1"
  sample_value "hello", "varchar(25)", "'hello'"
  sample_value 1.5_f32, "float", "1.5", type_safe_value: false
  sample_value 1.5, "double", "1.5"
  sample_value Time.new(2016, 2, 15), "datetime", "TIMESTAMP '2016-02-15 00:00:00.000'"
  sample_value Time.new(2016, 2, 15, 10, 15, 30), "datetime", "TIMESTAMP '2016-02-15 10:15:30.000'"
  sample_value Time.new(2016, 2, 15, 10, 15, 30), "timestamp", "TIMESTAMP '2016-02-15 10:15:30.000'"
  sample_value Time.new(2016, 2, 29), "date", "LAST_DAY('2016-02-15')", type_safe_value: false
  sample_value Time::Span.new(nanoseconds: 0), "Time", "TIME('00:00:00')"
  sample_value Time::Span.new(10, 25, 21), "Time", "TIME('10:25:21')"
  sample_value Time::Span.new(0, 0, 10, 5, 0), "Time", "TIME('00:10:05.000')"

  DB.open db_url do |db|
    # needs to check version, microsecond support >= 5.7
    dbversion = SemanticVersion.parse(db.scalar("SELECT VERSION();").as(String))
    if dbversion >= SemanticVersion.new(5, 7, 0)
      sample_value Time.new(2016, 2, 15, 10, 15, 30, nanosecond: 543_000_000), "datetime(3)", "TIMESTAMP '2016-02-15 10:15:30.543'"
      sample_value Time.new(2016, 2, 15, 10, 15, 30, nanosecond: 543_000_000), "timestamp(3)", "TIMESTAMP '2016-02-15 10:15:30.543'"
    end

    # zero dates http://dev.mysql.com/doc/refman/5.7/en/datetime.html - work on some mysql not others,
    # NO_ZERO_IN_DATE enabled as part of strict mode in MySQL 5.7.8. - http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-changes
    mode = db.scalar("SELECT @@sql_mode").as(String)
    if !mode.match(/NO_ZERO_DATE/)
      sample_value Time.new(0, 0, 0), "datetime", "TIMESTAMP '0000-00-00 00:00:00'"
    end
  end

  ary = UInt8[0x41, 0x5A, 0x61, 0x7A]
  sample_value Bytes.new(ary.to_unsafe, ary.size), "BLOB", "X'415A617A'", type_safe_value: false

  [
    {"TINYBLOB", 10},
    {"BLOB", 1000},
    {"MEDIUMBLOB", 10000},
    {"LONGBLOB", 100000},
  ].each do |type, size|
    sample_value Bytes.new((ary * size).to_unsafe, ary.size * size), type, "X'#{"415A617A" * size}'", type_safe_value: false
  end

  [
    {"TINYTEXT", 10},
    {"TEXT", 1000},
    {"MEDIUMTEXT", 10000},
    {"LONGTEXT", 100000},
  ].each do |type, size|
    value = "Ham Sandwich" * size
    sample_value value, type, "'#{value}'"
  end

  binding_syntax do |index|
    "?"
  end

  create_table_1column_syntax do |table_name, col1|
    "create table #{table_name} (#{col1.name} #{col1.sql_type} #{col1.null ? "NULL" : "NOT NULL"})"
  end

  create_table_2columns_syntax do |table_name, col1, col2|
    "create table #{table_name} (#{col1.name} #{col1.sql_type} #{col1.null ? "NULL" : "NOT NULL"}, #{col2.name} #{col2.sql_type} #{col2.null ? "NULL" : "NOT NULL"})"
  end

  select_1column_syntax do |table_name, col1|
    "select #{col1.name} from #{table_name}"
  end

  select_2columns_syntax do |table_name, col1, col2|
    "select #{col1.name}, #{col2.name} from #{table_name}"
  end

  select_count_syntax do |table_name|
    "select count(*) from #{table_name}"
  end

  select_scalar_syntax do |expression|
    "select #{expression}"
  end

  insert_1column_syntax do |table_name, col, expression|
    "insert into #{table_name} (#{col.name}) values (#{expression})"
  end

  insert_2columns_syntax do |table_name, col1, expr1, col2, expr2|
    "insert into #{table_name} (#{col1.name}, #{col2.name}) values (#{expr1}, #{expr2})"
  end

  drop_table_if_exists_syntax do |table_name|
    "drop table if exists #{table_name}"
  end

  it "gets last insert row id", prepared: :both do |db|
    db.exec "create table person (id int not null primary key auto_increment, name varchar(25), age int)"
    db.exec %(insert into person (name, age) values ("foo", 10))
    res = db.exec %(insert into person (name, age) values ("foo", 10))
    res.last_insert_id.should eq(2)
    res.rows_affected.should eq(1)
  end

  it "get timestamp from table" do |db|
    db.exec "create table table1 (m int, dt datetime, ts timestamp DEFAULT CURRENT_TIMESTAMP)"
    db.exec "insert into table1 (m, dt) values(?, NOW())", 1

    dt, ts = db.query_one "SELECT dt, ts from table1", as: {Time, Time}
    (ts - dt).total_seconds.should be_close(0.0, 0.5)
  end

  it "raises on unsupported param types" do |db|
    expect_raises Exception, "MySql::Type does not support NotSupportedType values" do
      db.query "select ?", NotSupportedType.new
    end
    # TODO raising exception does not close the connection and pool is exhausted
  end

  it "ensures statements are closed" do |db|
    db.exec %(create table if not exists a (i int not null, str text not null);)
    db.exec %(insert into a (i, str) values (23, "bai bai");)

    2.times do |i|
      DB.open db.uri do |db|
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

  it "does not close a connection before cleaning up the result set" do |db|
    begin
      DB.open db.uri do |db|
        db.query("select 'foo'") do |rs|
          rs.each do
            rs.read(String)
          end
          db.query("select 'bar'") do |rs|
            rs.each do
              rs.read(String)
            end
          end
        end
      end
    rescue e
      fail("Expected no exception, but got \"#{e.message}\"")
    end
  end

  it "does not close a connection before cleaning up the text result set" do |db|
    begin
      DB.open db.uri do |db|
        db.unprepared.query("select 'foo'") do |rs|
          rs.each do
            rs.read(String)
          end
          db.unprepared.query("select 'bar'") do |rs|
            rs.each do
              rs.read(String)
            end
          end
        end
      end
    rescue e
      fail("Expected no exception, but got \"#{e.message}\"")
    end
  end

  it "allows unprepared statement queries" do |db|
    db.exec %(create table if not exists a (i int not null, str text not null);)
    db.exec %(insert into a (i, str) values (23, "bai bai");)

    2.times do |i|
      DB.open db.uri do |db|
        begin
          db.unprepared.query("SELECT i, str FROM a WHERE i = 23") do |rs|
            rs.each do
              rs.read(Int32).should eq 23
              rs.read(String).should eq "bai bai"
            end
          end
        rescue e
          fail("Expected no exception, but got \"#{e.message}\"")
        end
      end
    end
  end
end
