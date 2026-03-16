require "openssl/sha1"
require "openssl/digest"

module MySql::Auth
  def self.compute_auth_response(plugin_name : String, password : String?, scramble : Bytes) : Bytes
    if password.nil? || password.empty?
      return Bytes.empty
    end

    case plugin_name
    when "mysql_native_password"
      native_password(password, scramble)
    when "caching_sha2_password"
      caching_sha2_password(password, scramble)
    when "sha256_password"
      Bytes[0x01]
    when "mysql_clear_password"
      clear_password(password)
    else
      raise MySql::Connection::PacketError.new("Unsupported auth plugin: #{plugin_name}")
    end
  end

  def self.native_password(password : String, scramble : Bytes) : Bytes
    sizet_20 = LibC::SizeT.new(20)
    sha1 = OpenSSL::SHA1.hash(password)
    sha1sha1 = OpenSSL::SHA1.hash(sha1.to_unsafe, sizet_20)

    buffer = Bytes.new(40)
    buffer[0, 20].copy_from(scramble[0, 20])
    buffer[20, 20].copy_from(sha1sha1.to_slice)

    sizet_40 = LibC::SizeT.new(40)
    buffer_sha1 = OpenSSL::SHA1.hash(buffer.to_unsafe, sizet_40)

    result = Bytes.new(20)
    20.times { |i| result[i] = sha1[i] ^ buffer_sha1[i] }
    result
  end

  def self.caching_sha2_password(password : String, scramble : Bytes) : Bytes
    hash1 = sha256(password.to_slice)
    hash2 = sha256(hash1)

    concat = Bytes.new(hash2.size + scramble.size)
    concat[0, hash2.size].copy_from(hash2)
    concat[hash2.size, scramble.size].copy_from(scramble)
    hash3 = sha256(concat)

    result = Bytes.new(32)
    32.times { |i| result[i] = hash1[i] ^ hash3[i] }
    result
  end

  def self.clear_password(password : String) : Bytes
    bytes = Bytes.new(password.bytesize + 1)
    bytes[0, password.bytesize].copy_from(password.to_slice)
    bytes[password.bytesize] = 0_u8
    bytes
  end

  def self.xor_password_scramble(password : String, scramble : Bytes) : Bytes
    pass_bytes = password.to_slice
    result = Bytes.new(pass_bytes.size + 1)
    pass_bytes.size.times do |i|
      result[i] = pass_bytes[i] ^ scramble[i % scramble.size]
    end
    result[pass_bytes.size] = 0_u8
    result
  end

  def self.rsa_encrypt_password(password : String, scramble : Bytes, pem_key : String) : Bytes
    raise "RSA encryption not yet implemented"
  end

  private def self.sha256(data : Bytes) : Bytes
    digest = OpenSSL::Digest.new("SHA256")
    digest.update(data)
    digest.final
  end
end
