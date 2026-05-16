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
      scramble = "12345678901234567890".to_slice
      result = MySql::Auth.native_password("secret", scramble)
      result.hexstring.should eq("0f8b9033e0897c0a8338ebe3dea9010dda47ab56")
    end

    it "matches reference vector for password='pass', scramble='abcdefghij1234567890'" do
      scramble = "abcdefghij1234567890".to_slice
      result = MySql::Auth.native_password("pass", scramble)
      result.hexstring.should eq("97406e93d6b93db22d075b4f0f82e72168da5583")
    end

    it "produces 20-byte output" do
      result = MySql::Auth.native_password("x", "12345678901234567890".to_slice)
      result.size.should eq(20)
    end
  end

  describe ".caching_sha2_password" do
    it "matches reference vector for password='secret', scramble='12345678901234567890'" do
      scramble = "12345678901234567890".to_slice
      result = MySql::Auth.caching_sha2_password("secret", scramble)
      result.hexstring.should eq("51ecd6dedbd34d5445c0a190d4f51acf0d23b94db66c91f3f789faa9193751cd")
    end

    it "produces 32-byte output" do
      result = MySql::Auth.caching_sha2_password("x", "12345678901234567890".to_slice)
      result.size.should eq(32)
    end
  end

  describe ".clear_password" do
    it "appends null terminator" do
      MySql::Auth.clear_password("foo").should eq(Bytes[0x66, 0x6f, 0x6f, 0x00])
    end

    it "handles empty password" do
      MySql::Auth.clear_password("").should eq(Bytes[0x00])
    end
  end

  describe ".xor_password_scramble" do
    it "XORs password with cycled scramble and null-terminates with XOR" do
      result = MySql::Auth.xor_password_scramble("foo", "123456789012345678901234".to_slice)
      result.hexstring.should eq("575d5c34")
    end

    it "cycles scramble when password is longer" do
      result = MySql::Auth.xor_password_scramble("abcdefghijklmnop12345", "12345678901234567890".to_slice)
      result.hexstring.should eq("5050505050505050505a5a5e5e5a5a46060a0a040432")
    end
  end

  describe ".compute_auth_response" do
    it "returns empty bytes when password is nil" do
      scramble = "12345678901234567890".to_slice
      MySql::Auth.compute_auth_response("mysql_native_password", nil, scramble).should eq(Bytes.empty)
    end

    it "returns empty bytes when password is empty string" do
      scramble = "12345678901234567890".to_slice
      MySql::Auth.compute_auth_response("mysql_native_password", "", scramble).should eq(Bytes.empty)
    end

    it "dispatches to native_password for mysql_native_password" do
      scramble = "12345678901234567890".to_slice
      result = MySql::Auth.compute_auth_response("mysql_native_password", "secret", scramble)
      result.hexstring.should eq("0f8b9033e0897c0a8338ebe3dea9010dda47ab56")
    end

    it "dispatches to caching_sha2_password for caching_sha2_password" do
      scramble = "12345678901234567890".to_slice
      result = MySql::Auth.compute_auth_response("caching_sha2_password", "secret", scramble)
      result.hexstring.should eq("51ecd6dedbd34d5445c0a190d4f51acf0d23b94db66c91f3f789faa9193751cd")
    end

    it "returns [0x01] for sha256_password without TLS (key request)" do
      scramble = "12345678901234567890".to_slice
      MySql::Auth.compute_auth_response("sha256_password", "secret", scramble, ssl_established: false).should eq(Bytes[0x01])
    end

    it "returns plaintext+null for sha256_password over TLS" do
      scramble = "12345678901234567890".to_slice
      MySql::Auth.compute_auth_response("sha256_password", "foo", scramble, ssl_established: true).should eq(Bytes[0x66, 0x6f, 0x6f, 0x00])
    end

    it "dispatches to clear_password for mysql_clear_password" do
      scramble = "12345678901234567890".to_slice
      MySql::Auth.compute_auth_response("mysql_clear_password", "foo", scramble).should eq(Bytes[0x66, 0x6f, 0x6f, 0x00])
    end

    it "raises on unsupported plugin" do
      scramble = "12345678901234567890".to_slice
      expect_raises(MySql::Connection::PacketError, /Unsupported auth plugin/) do
        MySql::Auth.compute_auth_response("nonexistent_plugin", "x", scramble)
      end
    end
  end
end
