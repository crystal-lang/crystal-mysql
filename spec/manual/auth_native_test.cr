require "./spec_helper"

describe "mysql_native_password authentication" do
  it "mysql57 root no password" do
    assert_connects "mysql://root@#{DB_HOST_MYSQL57}"
  end
  it "mysql8_native root no password" do
    assert_connects "mysql://root@#{DB_HOST_MYSQL8_NATIVE}"
  end
  it "native_user with password" do
    assert_connects "mysql://native_user:native_pass@#{DB_HOST_MYSQL8_NATIVE}"
  end
end
