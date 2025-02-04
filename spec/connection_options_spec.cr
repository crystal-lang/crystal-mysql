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

private def ssl_from_params(params)
  Connection::SSLOptions.from_params(URI::Params.parse(params))
end

SSL_OPTION_PREFERRED = Connection::SSLOptions.new(mode: :preferred, key: nil, cert: nil, ca: nil)

describe Connection::Options do
  describe ".from_uri" do
    it "parses mysql://user@host/db" do
      from_uri("mysql://root@localhost/test").should eq(
        Connection::Options.new(
          transport: tcp("localhost", 3306),
          username: "root",
          password: nil,
          initial_catalog: "test",
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql://host" do
      from_uri("mysql://localhost").should eq(
        Connection::Options.new(
          transport: tcp("localhost", 3306),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql://host:port" do
      from_uri("mysql://localhost:1234").should eq(
        Connection::Options.new(
          transport: tcp("localhost", 1234),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses ?encoding=..." do
      from_uri("mysql://localhost:1234?encoding=utf8mb4_unicode_520_ci").should eq(
        Connection::Options.new(
          transport: tcp("localhost", 1234),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: "utf8mb4_unicode_520_ci",
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql://user@host?database=db" do
      from_uri("mysql://root@localhost?database=test").should eq(
        Connection::Options.new(
          transport: tcp("localhost", 3306),
          username: "root",
          password: nil,
          initial_catalog: "test",
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql:///path/to/socket" do
      from_uri("mysql:///path/to/socket").should eq(
        Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql:///path/to/socket?database=test" do
      from_uri("mysql:///path/to/socket?database=test").should eq(
        Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: nil,
          password: nil,
          initial_catalog: "test",
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql:///path/to/socket?encoding=utf8mb4_unicode_520_ci" do
      from_uri("mysql:///path/to/socket?encoding=utf8mb4_unicode_520_ci").should eq(
        Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: nil,
          password: nil,
          initial_catalog: nil,
          charset: "utf8mb4_unicode_520_ci",
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end

    it "parses mysql://user:pass@/path/to/socket?database=test" do
      from_uri("mysql://root:password@/path/to/socket?database=test").should eq(
        Connection::Options.new(
          transport: socket("/path/to/socket"),
          username: "root",
          password: "password",
          initial_catalog: "test",
          charset: Collations.default_collation,
          ssl_options: SSL_OPTION_PREFERRED
        )
      )
    end
  end
end

describe Connection::SSLOptions do
  describe ".from_params" do
    it "default is ssl-mode=preferred" do
      ssl_from_params("").mode.should eq(Connection::SSLMode::Preferred)
    end

    it "parses ssl-mode=preferred" do
      ssl_from_params("ssl-mode=preferred").mode.should eq(Connection::SSLMode::Preferred)
      ssl_from_params("ssl-mode=Preferred").mode.should eq(Connection::SSLMode::Preferred)
      ssl_from_params("ssl-mode=PREFERRED").mode.should eq(Connection::SSLMode::Preferred)
    end

    it "parses ssl-mode=disabled" do
      ssl_from_params("ssl-mode=disabled").mode.should eq(Connection::SSLMode::Disabled)
      ssl_from_params("ssl-mode=Disabled").mode.should eq(Connection::SSLMode::Disabled)
      ssl_from_params("ssl-mode=DISABLED").mode.should eq(Connection::SSLMode::Disabled)
    end

    it "parses ssl-mode=verifyca" do
      ssl_from_params("ssl-mode=verifyca").mode.should eq(Connection::SSLMode::VerifyCA)
      ssl_from_params("ssl-mode=verify-ca").mode.should eq(Connection::SSLMode::VerifyCA)
      ssl_from_params("ssl-mode=verify_ca").mode.should eq(Connection::SSLMode::VerifyCA)
      ssl_from_params("ssl-mode=VERIFY_CA").mode.should eq(Connection::SSLMode::VerifyCA)
    end

    it "parses ssl-mode=verifyidentity" do
      ssl_from_params("ssl-mode=verifyidentity").mode.should eq(Connection::SSLMode::VerifyIdentity)
      ssl_from_params("ssl-mode=verify-identity").mode.should eq(Connection::SSLMode::VerifyIdentity)
      ssl_from_params("ssl-mode=verify_identity").mode.should eq(Connection::SSLMode::VerifyIdentity)
      ssl_from_params("ssl-mode=VERIFY_IDENTITY").mode.should eq(Connection::SSLMode::VerifyIdentity)
    end

    it "parses ssl-key, ssl-cert, ssl-ca" do
      ssl_from_params("ssl-key=path/to/key.pem&ssl-cert=path/to/cert.pem&ssl-ca=path/to/ca.pem").should eq(
        Connection::SSLOptions.new(mode: Connection::SSLMode::Preferred,
          key: Path["path/to/key.pem"],
          cert: Path["path/to/cert.pem"],
          ca: Path["path/to/ca.pem"])
      )
    end

    it "missing ssl-key, ssl-cert, ssl-ca as nil" do
      ssl_from_params("").should eq(
        Connection::SSLOptions.new(mode: Connection::SSLMode::Preferred,
          key: nil,
          cert: nil,
          ca: nil)
      )
    end
  end
end
