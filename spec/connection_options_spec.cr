require "./spec_helper"

private def from_uri(uri)
  Connection::Options.from_uri(URI.parse(uri))
end

describe Connection::Options do
  describe ".from_uri" do
    it "parses mysql://user@host/db" do
      from_uri("mysql://root@localhost/test").should eq(
        MySql::Connection::Options.new(
          host: "localhost",
          port: 3306,
          username: "root",
          password: nil,
          initial_catalog: "test",
          charset: Collations.default_collation
        )
      )
    end

    it "parses mysql://host" do
      from_uri("mysql://localhost").should eq(
        MySql::Connection::Options.new(
          host: "localhost",
          port: 3306,
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: Collations.default_collation
        )
      )
    end

    it "parses mysql://host:port" do
      from_uri("mysql://localhost:1234").should eq(
        MySql::Connection::Options.new(
          host: "localhost",
          port: 1234,
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: Collations.default_collation
        )
      )
    end

    it "parses ?encoding=..." do
      from_uri("mysql://localhost:1234?encoding=utf8mb4_unicode_520_ci").should eq(
        MySql::Connection::Options.new(
          host: "localhost",
          port: 1234,
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: "utf8mb4_unicode_520_ci"
        )
      )
    end
  end
end
