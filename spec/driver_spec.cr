require "./spec_helper"

def with_db(&block : DB::Database ->)
  DB.open db_url, &block
end

describe Driver do
  it "should register mysql name" do
    DB.driver_class("mysql").should eq(MySql::Driver)
  end

  it "should connect with credentials" do
    with_db do |db|
      db.scalar("SELECT DATABASE()").should be_nil
      db.scalar("SELECT CURRENT_USER()").should match(/^root@/)

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

    DB.open "mysql://crystal_test:secret@#{database_host}/crystal_mysql_test" do |db|
      db.scalar("SELECT DATABASE()").should eq("crystal_mysql_test")
      db.scalar("SELECT CURRENT_USER()").should match(/^crystal_test@/)
    end

    with_db do |db|
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    end
  end

  it "create and drop test database" do
    sql = "SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'crystal_mysql_test'"

    with_db do |db|
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
      db.exec "CREATE DATABASE crystal_mysql_test"
      DB.open db_url("crystal_mysql_test") do |db|
        db.scalar(sql).should eq(1)
        db.scalar("SELECT DATABASE()").should eq("crystal_mysql_test")
      end
      db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    end

    with_db do |db|
      db.scalar(sql).should eq(0)
      db.scalar("SELECT DATABASE()").should be_nil
    end
  end
end
