# Target: mysql80 (port 13307)
# Run:    crystal run spec/manual/auth_sha256_test.cr
#
# Tests sha256_password authentication.
# Expected to FAIL with current code.

require "../../src/mysql"

tests = [
  {"mysql://sha256_user:sha256_pass@localhost:13307", "sha256_user over TLS"},
  {"mysql://sha256_user:sha256_pass@localhost:13307?ssl-mode=disabled", "sha256_user without TLS"},
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
