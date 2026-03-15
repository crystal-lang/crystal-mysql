# Target: mysql80 (port 13307), mysql80_native (port 13308)
# Run:    crystal run spec/manual/auth_switch_test.cr
#
# Tests AuthSwitchRequest handling.
# Expected to FAIL with current code.

require "../../src/mysql"

tests = [
  {"mysql://native_user:native_pass@localhost:13307", "auth switch csha2 -> native"},
  {"mysql://csha2_user:csha2_pass@localhost:13308", "auth switch native -> csha2"},
]

tests.each do |url, label|
  begin
    DB.open(url) do |db|
      user = db.scalar("SELECT CURRENT_USER()").as(String)
      puts "PASS: #{label} (user=#{user})"
    end
  rescue ex
    puts "FAIL: #{label} — #{ex.message}"
  end
end
