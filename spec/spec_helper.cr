require "spec"
require "../src/mysql"
require "semantic_version"

include MySql

def db_url(initial_db = nil)
  if initial_db
    "mysql://root@#{database_host}?database=#{initial_db}"
  else
    "mysql://root@#{database_host}"
  end
end

def database_host
  ENV.fetch("DATABASE_HOST", "localhost")
end

def with_db(database_name, options = nil, &block : DB::Database ->)
  DB.open db_url do |db|
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
    db.exec "CREATE DATABASE crystal_mysql_test"
  end

  DB.open "#{db_url(database_name)}&#{options}", &block
ensure
  DB.open db_url do |db|
    db.exec "DROP DATABASE IF EXISTS crystal_mysql_test"
  end
end

def mysql_version(db) : SemanticVersion
  # some docker images might report 5.7.30-0ubuntu0.18.04.1, so we split in "-"
  SemanticVersion.parse(db.scalar("SELECT VERSION();").as(String).split("-").first)
end
