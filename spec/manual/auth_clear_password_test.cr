# mysql_clear_password requires server-side plugin (PAM/LDAP) that requests it.
# Our docker setup doesn't trigger this path, so this is a baseline connectivity test.

require "./spec_helper"

describe "mysql_clear_password authentication" do
  it "baseline connectivity" do
    assert_connects "mysql://root@#{DB_HOST_MYSQL57}"
  end
end
