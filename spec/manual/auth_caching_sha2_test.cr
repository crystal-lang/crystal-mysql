require "./spec_helper"

describe "caching_sha2_password authentication" do
  it "mysql80 root no password (csha2 default)" do
    assert_connects "mysql://root@#{DB_HOST_MYSQL8}"
  end
  it "csha2_user with password" do
    assert_connects "mysql://csha2_user:csha2_pass@#{DB_HOST_MYSQL8}"
  end
  it "nopass_user empty password" do
    assert_connects "mysql://nopass_user@#{DB_HOST_MYSQL8}"
  end
  it "mysql90 root no password" do
    assert_connects "mysql://root@#{DB_HOST_MYSQL9}"
  end
  it "mysql90 csha2_user" do
    assert_connects "mysql://csha2_user:csha2_pass@#{DB_HOST_MYSQL9}"
  end
end
