require "spec"
require "../src/mysql"

include MySql

def db_url(initial_db = nil)
  "mysql://root@#{database_host}/#{initial_db}"
end

def database_host
  ENV.fetch("DATABASE_HOST", "localhost")
end

def with_db(database_name, options = nil, &block : DB::Database ->)
  DB.open db_url do |db|
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    db.exec "CREATE DATABASE crystal_mysql_test"
  end

  DB.open "#{db_url(database_name)}?#{options}", &block
ensure
  DB.open db_url do |db|
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
  end
end
