# Target: mysql80 (port 13307)
# Run:    crystal run spec/manual/auth_clear_password_test.cr
#
# mysql_clear_password requires server-side plugin (PAM/LDAP) that requests it.
# Our docker setup doesn't trigger this path, so this is a baseline connectivity test.

require "../../src/mysql"

tests = [
  {"mysql://root@localhost:13307", "baseline connectivity"},
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
