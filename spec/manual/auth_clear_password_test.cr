# Target: mysql80 (port 13307)
# Run:    crystal spec spec/manual/auth_clear_password_test.cr
#
# mysql_clear_password requires server-side plugin (PAM/LDAP) that requests it.
# Our docker setup doesn't trigger this path, so this is a baseline connectivity test.

require "spec"
require "../../src/mysql"

describe "mysql_clear_password authentication" do
  [
    {"mysql://root@localhost:13307", "baseline connectivity"},
  ].each do |(url, label)|
    it label do
      DB.open(url) do |db|
        db.scalar("SELECT CURRENT_USER()").as(String).should_not be_empty
      end
    end
  end
end
