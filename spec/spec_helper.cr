require "spec"
require "../src/mysql"

include MySql

def db_url(initial_db = nil)
  "mysql://#{database_user}:#{database_password}@#{database_host}/#{initial_db}"
end

def database_host
  ENV.fetch("DATABASE_HOST", "localhost")
end

def database_user
  ENV.fetch("DATABASE_USER", "root")
end

def database_password
  ENV.fetch("DATABASE_PASSWORD", "crystal_mysql_spec")
end
