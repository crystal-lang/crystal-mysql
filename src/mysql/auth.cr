require "digest/sha256"

module MySql::Auth
  def self.compute_auth_response(plugin_name : String, password : String?, scramble : Bytes, & : Bytes -> _)
    if password.nil? || password.empty?
      return yield Bytes.empty
    end

    case plugin_name
    when "mysql_native_password"
      native_password(password, scramble) do |auth_response|
        yield auth_response
      end
    when "caching_sha2_password"
      caching_sha2_password(password, scramble) do |auth_response|
        yield auth_response
      end
    when "mysql_clear_password"
      clear_password(password) do |auth_response|
        yield auth_response
      end
    else
      raise MySql::Connection::PacketError.new("Unsupported auth plugin: #{plugin_name}")
    end
  end

  def self.native_password(password : String, scramble : Bytes, & : Bytes -> _)
    sizet_20 = LibC::SizeT.new(20)
    sha1 = OpenSSL::SHA1.hash(password)
    sha1sha1 = OpenSSL::SHA1.hash(sha1.to_unsafe, sizet_20)

    buffer = uninitialized UInt8[40]
    buffer.to_unsafe.copy_from(scramble.to_unsafe, 20)
    (buffer.to_unsafe + 20).copy_from(sha1sha1.to_unsafe, 20)

    sizet_40 = LibC::SizeT.new(40)
    buffer_sha1 = OpenSSL::SHA1.hash(buffer.to_unsafe, sizet_40)

    # reuse buffer
    20.times { |i|
      buffer[i] = sha1[i] ^ buffer_sha1[i]
    }

    yield Bytes.new(buffer.to_unsafe, 20)
  end

  def self.clear_password(password : String, & : Bytes -> _)
    # Duping the string as a slice. MySQL protocol expects the trailing null byte to be included.
    yield Bytes.new(password.to_unsafe, password.bytesize + 1).dup
  end

  def self.caching_sha2_password(password : String, scramble : Bytes, & : Bytes -> _)
    pw_hash = Digest::SHA256.digest(password.to_slice)

    digest = Digest::SHA256.new
    digest << Digest::SHA256.digest(pw_hash)
    digest << scramble
    combined_hash = digest.final

    buffer = uninitialized UInt8[32]
    32.times { |i| buffer[i] = pw_hash[i] ^ combined_hash[i] }
    yield buffer.to_slice
  end
end
