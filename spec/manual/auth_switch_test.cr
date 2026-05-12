# Target: mysql80 (port 13307), mysql80_native (port 13308)
# Run:    crystal spec spec/manual/auth_switch_test.cr
#
# Tests AuthSwitchRequest handling.

require "spec"
require "../../src/mysql"

describe "AuthSwitchRequest handling" do
  [
    {"mysql://native_user:native_pass@localhost:13307", "auth switch csha2 -> native"},
    {"mysql://csha2_user:csha2_pass@localhost:13308", "auth switch native -> csha2"},
  ].each do |(url, label)|
    it label do
      DB.open(url) do |db|
        db.scalar("SELECT CURRENT_USER()").as(String).should_not be_empty
      end
    end
  end
end
