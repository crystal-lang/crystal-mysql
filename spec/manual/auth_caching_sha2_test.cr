# Target: mysql80 (port 13307), mysql90 (port 13309)
# Run:    crystal run spec/manual/auth_caching_sha2_test.cr
#
# Tests caching_sha2_password authentication.
# Expected to FAIL with current code.

require "../../src/mysql"

tests = [
  {"mysql://root@localhost:13307", "mysql80 root no password (csha2 default)"},
  {"mysql://csha2_user:csha2_pass@localhost:13307", "csha2_user with password"},
  {"mysql://nopass_user@localhost:13307", "nopass_user empty password"},
  {"mysql://root@localhost:13309", "mysql90 root no password"},
  {"mysql://csha2_user:csha2_pass@localhost:13309", "mysql90 csha2_user"},
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
