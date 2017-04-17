require "spec"
require "../src/mysql"

include MySql

def db_url(initial_db = nil)
  "mysql://root@localhost/#{initial_db}"
end
