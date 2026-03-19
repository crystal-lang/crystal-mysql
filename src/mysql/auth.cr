require "openssl/sha1"
require "openssl/digest"

# Reopen Crystal's LibCrypto to add RSA encryption bindings for MySQL auth
lib LibCrypto
  fun bio_new_mem_buf = BIO_new_mem_buf(buf : Void*, len : Int32) : Bio*
  fun pem_read_bio_pubkey = PEM_read_bio_PUBKEY(bio : Bio*, x : Void*, cb : Void*, u : Void*) : Void*
  fun evp_pkey_free = EVP_PKEY_free(pkey : Void*)
  fun evp_pkey_ctx_new = EVP_PKEY_CTX_new(pkey : Void*, e : Void*) : Void*
  fun evp_pkey_ctx_free = EVP_PKEY_CTX_free(ctx : Void*)
  fun evp_pkey_encrypt_init = EVP_PKEY_encrypt_init(ctx : Void*) : Int32
  fun evp_pkey_ctx_ctrl = EVP_PKEY_CTX_ctrl(ctx : Void*, keytype : Int32, optype : Int32, cmd : Int32, p1 : Int32, p2 : Void*) : Int32
  fun evp_pkey_encrypt = EVP_PKEY_encrypt(ctx : Void*, out : UInt8*, outlen : LibC::SizeT*, in_buf : UInt8*, inlen : LibC::SizeT) : Int32
end

module MySql::Auth
  def self.compute_auth_response(plugin_name : String, password : String?, scramble : Bytes, ssl_established : Bool = false) : Bytes
    if password.nil? || password.empty?
      return Bytes.empty
    end

    case plugin_name
    when "mysql_native_password"
      native_password(password, scramble)
    when "caching_sha2_password"
      caching_sha2_password(password, scramble)
    when "sha256_password"
      if ssl_established
        # Over TLS: send plaintext password null-terminated
        clear_password(password)
      else
        # Without TLS: send 0x01 to request RSA public key
        Bytes[0x01]
      end
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
    xored = xor_password_scramble(password, scramble)

    bio = LibCrypto.bio_new_mem_buf(pem_key.to_unsafe.as(Void*), pem_key.bytesize)
    raise Connection::PacketError.new("Failed to create BIO for RSA key") if bio.null?

    begin
      pkey = LibCrypto.pem_read_bio_pubkey(bio, nil, nil, nil)
      raise Connection::PacketError.new("Failed to parse RSA public key") if pkey.null?

      begin
        ctx = LibCrypto.evp_pkey_ctx_new(pkey, nil)
        raise Connection::PacketError.new("Failed to create EVP_PKEY_CTX") if ctx.null?

        begin
          if LibCrypto.evp_pkey_encrypt_init(ctx) <= 0
            raise Connection::PacketError.new("EVP_PKEY_encrypt_init failed")
          end

          # Set RSA OAEP padding
          # EVP_PKEY_RSA=6, EVP_PKEY_OP_ENCRYPT=1<<9, EVP_PKEY_CTRL_RSA_PADDING=0x1001, RSA_PKCS1_OAEP_PADDING=4
          if LibCrypto.evp_pkey_ctx_ctrl(ctx, 6, 1 << 9, 0x1001, 4, nil) <= 0
            raise Connection::PacketError.new("Failed to set RSA OAEP padding")
          end

          # Determine output size
          out_len = LibC::SizeT.new(0)
          if LibCrypto.evp_pkey_encrypt(ctx, nil, pointerof(out_len), xored.to_unsafe, LibC::SizeT.new(xored.size)) <= 0
            raise Connection::PacketError.new("EVP_PKEY_encrypt size determination failed")
          end

          # Encrypt
          encrypted = Bytes.new(out_len.to_i32)
          if LibCrypto.evp_pkey_encrypt(ctx, encrypted.to_unsafe, pointerof(out_len), xored.to_unsafe, LibC::SizeT.new(xored.size)) <= 0
            raise Connection::PacketError.new("RSA encryption failed")
          end

          encrypted[0, out_len.to_i32]
        ensure
          LibCrypto.evp_pkey_ctx_free(ctx)
        end
      ensure
        LibCrypto.evp_pkey_free(pkey)
      end
    ensure
      LibCrypto.BIO_free(bio)
    end
  end

  private def self.sha256(data : Bytes) : Bytes
    digest = OpenSSL::Digest.new("SHA256")
    digest.update(data)
    digest.final
  end
end
