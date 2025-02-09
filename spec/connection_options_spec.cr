require "./spec_helper"

private def from_uri(uri)
  Connection::Options.from_uri(URI.parse(uri))
end

private def tcp(host, port)
  URI.new("tcp", host, port)
end

private def socket(path)
  URI.new("unix", nil, nil, path)
end

describe Connection::Options do
  describe ".from_uri" do
    it "parses mysql://user@host/db" do
      from_uri("mysql://root@localhost/test").should eq(
        MySql::Connection::Options.new(
          transport: tcp("localhost", 3306),
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
          transport: tcp("localhost", 3306),
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
          transport: tcp("localhost", 1234),
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
          transport: tcp("localhost", 1234),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: "utf8mb4_unicode_520_ci"
        )
      )
    end

    it "parses mysql://user@host?database=db" do
      from_uri("mysql://root@localhost?database=test").should eq(
        MySql::Connection::Options.new(
          transport: tcp("localhost", 3306),
          username: "root",
          password: nil,
          initial_catalog: "test",
          charset: Collations.default_collation
        )
      )
    end

    it "parses mysql:///path/to/socket" do
      from_uri("mysql:///path/to/socket").should eq(
        MySql::Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: Collations.default_collation
        )
      )
    end

    it "parses mysql:///path/to/socket?database=test" do
      from_uri("mysql:///path/to/socket?database=test").should eq(
        MySql::Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: nil,
          password: nil,
          initial_catalog: "test",
          charset: Collations.default_collation
        )
      )
    end

    it "parses mysql:///path/to/socket?encoding=utf8mb4_unicode_520_ci" do
      from_uri("mysql:///path/to/socket?encoding=utf8mb4_unicode_520_ci").should eq(
        MySql::Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: "utf8mb4_unicode_520_ci"
        )
      )
    end

    it "parses mysql://user:pass@/path/to/socket?database=test" do
      from_uri("mysql://root:password@/path/to/socket?database=test").should eq(
        MySql::Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: "root",
          password: "password",
          initial_catalog: "test",
          charset: Collations.default_collation
        )
      )
    end
  end
end
