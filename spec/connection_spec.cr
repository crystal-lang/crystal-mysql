require "./spec_helper"

describe Connection do
  it "reports the driver name" do
    DB.connect db_url do |cnn|
      cnn.driver_name.should eq("mysql")
    end
  end

  it "reports the server version from the handshake" do
    DB.connect db_url do |cnn|
      # MariaDB < 11 prefixes the handshake version with "5.5.5-", which `VERSION()` omits
      cnn.server_version.not_nil!.should end_with(cnn.scalar("SELECT VERSION()").as(String))
    end
  end

  it "reports the server name" do
    DB.connect db_url do |cnn|
      expected = cnn.scalar("SELECT VERSION()").as(String).includes?("MariaDB") ? "MariaDB" : "MySQL"
      cnn.server_name.should eq(expected)
    end
  end
end
