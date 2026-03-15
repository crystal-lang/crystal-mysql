# Target: mysql57 (port 13306), mysql80_native (port 13308)
# Run:    crystal run spec/manual/auth_native_test.cr

require "../../src/mysql"

tests = [
  {"mysql://root@localhost:13306", "mysql57 root no password"},
  {"mysql://root@localhost:13308", "mysql80_native root no password"},
  {"mysql://native_user:native_pass@localhost:13308", "native_user with password"},
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
