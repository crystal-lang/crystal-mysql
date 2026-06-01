require "./spec_helper"

# Reference vectors computed independently using openssl + python XOR.
# Source inputs are documented per-test; re-derivable as:
#   printf '%s' "<password>" | openssl dgst -sha1 -binary > stage1
#   openssl dgst -sha1 -binary < stage1 > stage2
#   { printf '%s' "<scramble>"; cat stage2; } | openssl dgst -sha1 -binary > intermediate
#   result = stage1 XOR intermediate
describe MySql::Auth do
  describe ".native_password" do
    it "matches reference vector for password='secret', scramble='12345678901234567890'" do
      MySql::Auth.native_password("secret", "12345678901234567890".to_slice,
        &.hexstring.should(eq("0f8b9033e0897c0a8338ebe3dea9010dda47ab56"))
      )
    end

    it "matches reference vector for password='pass', scramble='abcdefghij1234567890'" do
      MySql::Auth.native_password("pass", "abcdefghij1234567890".to_slice,
        &.hexstring.should(eq("97406e93d6b93db22d075b4f0f82e72168da5583"))
      )
    end
  end

  describe ".compute_auth_response" do
    it "raises on unsupported plugin" do
      expect_raises(MySql::Connection::PacketError, /Unsupported auth plugin/) do
        MySql::Auth.compute_auth_response("nonexistent_plugin", "x", "12345678901234567890".to_slice) {}
      end
    end

    it "returns empty bytes when password is nil" do
      MySql::Auth.compute_auth_response("mysql_native_password", nil, "12345678901234567890".to_slice,
        &.should(eq(Bytes.empty))
      )
    end

    it "returns empty bytes when password is empty string" do
      MySql::Auth.compute_auth_response("mysql_native_password", "", "12345678901234567890".to_slice,
        &.should(eq(Bytes.empty))
      )
    end

    it "dispatches to native_password for mysql_native_password" do
      MySql::Auth.compute_auth_response("mysql_native_password", "secret", "12345678901234567890".to_slice,
        &.hexstring.should(eq("0f8b9033e0897c0a8338ebe3dea9010dda47ab56"))
      )
    end
  end
end
