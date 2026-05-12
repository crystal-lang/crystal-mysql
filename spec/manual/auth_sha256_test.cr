# Target: mysql80 (port 13307)
# Run:    crystal spec spec/manual/auth_sha256_test.cr
#
# Tests sha256_password authentication.

require "spec"
require "../../src/mysql"

describe "sha256_password authentication" do
  [
    {"mysql://sha256_user:sha256_pass@localhost:13307", "sha256_user over TLS"},
    {"mysql://sha256_user:sha256_pass@localhost:13307?ssl-mode=disabled", "sha256_user without TLS"},
  ].each do |(url, label)|
    it label do
      DB.open(url) do |db|
        db.scalar("SELECT CURRENT_USER()").as(String).should_not be_empty
      end
    end
  end
end
