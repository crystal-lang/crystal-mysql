# Target: mysql57 (port 13306), mysql80_native (port 13308)
# Run:    crystal spec spec/manual/auth_native_test.cr

require "spec"
require "../../src/mysql"

describe "mysql_native_password authentication" do
  [
    {"mysql://root@localhost:13306", "mysql57 root no password"},
    {"mysql://root@localhost:13308", "mysql80_native root no password"},
    {"mysql://native_user:native_pass@localhost:13308", "native_user with password"},
  ].each do |(url, label)|
    it label do
      DB.open(url) do |db|
        db.scalar("SELECT CURRENT_USER()").as(String).should_not be_empty
      end
    end
  end
end
