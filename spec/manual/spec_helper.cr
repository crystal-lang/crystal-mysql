require "spec"
require "../../src/mysql"

DB_HOST_MYSQL57 = ENV.fetch("DB_HOST_MYSQL57", "localhost:13306")
DB_HOST_MYSQL8 = ENV.fetch("DB_HOST_MYSQL8", "localhost:13307")
DB_HOST_MYSQL8_NATIVE = ENV.fetch("DB_HOST_MYSQL8_NATIVE", "localhost:13308")
DB_HOST_MYSQL9 = ENV.fetch("DB_HOST_MYSQL9", "localhost:13309")
DB_HOST_MARIADB11 = ENV.fetch("DB_HOST_MARIADB11", "localhost:13311")

def assert_connects(url, *, file = __FILE__, line = __LINE__)
  DB.open(url) do |db|
    db.scalar("SELECT CURRENT_USER()").as(String).should_not be_empty, file: file, line: line
  end
end
