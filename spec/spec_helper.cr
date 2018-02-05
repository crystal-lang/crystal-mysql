require "spec"
require "../src/mysql"

include MySql

def db_url(initial_db = nil)
  "mysql://root@#{database_host}/#{initial_db}"
end

def database_host
  ENV.fetch("DATABASE_HOST", "localhost")
end
