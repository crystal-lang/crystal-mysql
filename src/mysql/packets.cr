require "openssl/sha1"

module MySql::Protocol
  struct HandshakeV10
    getter auth_plugin_data : Bytes

    def initialize(@auth_plugin_data)
    end

    def self.read(packet : MySql::ReadPacket)
      protocol_version = packet.read_byte!
      version = packet.read_string
      thread = packet.read_int

      auth_data = Bytes.new(20)
      packet.read(auth_data[0, 8])
      packet.read_byte!
      cap1 = packet.read_byte!
      cap2 = packet.read_byte!
      charset = packet.read_byte!
      packet.read_byte_array(2)
      cap3 = packet.read_byte!
      cap4 = packet.read_byte!

      auth_plugin_data_length = packet.read_byte!
      packet.read_byte_array(10)
      packet.read(auth_data[8, {13, auth_plugin_data_length.to_i16 - 8}.max - 1])
      packet.read_byte!
      packet.read_string

      HandshakeV10.new(auth_data)
    end
  end

  struct HandshakeResponse41
    CLIENT_LONG_PASSWORD                  = 0x00000001
    CLIENT_FOUND_ROWS                     = 0x00000002
    CLIENT_LONG_FLAG                      = 0x00000004
    CLIENT_CONNECT_WITH_DB                = 0x00000008
    CLIENT_NO_SCHEMA                      = 0x00000010
    CLIENT_COMPRESS                       = 0x00000020
    CLIENT_ODBC                           = 0x00000040
    CLIENT_LOCAL_FILES                    = 0x00000080
    CLIENT_IGNORE_SPACE                   = 0x00000100
    CLIENT_PROTOCOL_41                    = 0x00000200
    CLIENT_INTERACTIVE                    = 0x00000400
    CLIENT_SSL                            = 0x00000800
    CLIENT_IGNORE_SIGPIPE                 = 0x00001000
    CLIENT_TRANSACTIONS                   = 0x00002000
    CLIENT_RESERVED                       = 0x00004000
    CLIENT_SECURE_CONNECTION              = 0x00008000
    CLIENT_MULTI_STATEMENTS               = 0x00010000
    CLIENT_MULTI_RESULTS                  = 0x00020000
    CLIENT_PS_MULTI_RESULTS               = 0x00040000
    CLIENT_PLUGIN_AUTH                    = 0x00080000
    CLIENT_CONNECT_ATTRS                  = 0x00100000
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 0x00400000
    CLIENT_SESSION_TRACK                  = 0x00800000
    CLIENT_DEPRECATE_EOF                  = 0x01000000

    def initialize(@username : String?, @password : String?, @initial_catalog : String?, @auth_plugin_data : Bytes)
    end

    def write(packet : MySql::WritePacket)
      caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA

      caps |= CLIENT_PLUGIN_AUTH if @password

      caps |= CLIENT_CONNECT_WITH_DB if @initial_catalog

      packet.write_bytes caps, IO::ByteFormat::LittleEndian

      packet.write_bytes 0x00000000u32, IO::ByteFormat::LittleEndian
      packet.write_byte 0x21u8 # utf8_general_ci
      23.times { packet.write_byte 0_u8 }

      packet << @username
      packet.write_byte 0_u8

      if password = @password
        sizet_20 = LibC::SizeT.new(20)
        sha1 = OpenSSL::SHA1.hash(password)
        sha1sha1 = OpenSSL::SHA1.hash(sha1.to_unsafe, sizet_20)

        buffer = uninitialized UInt8[40]
        buffer.to_unsafe.copy_from(@auth_plugin_data.to_unsafe, 20)
        (buffer.to_unsafe + 20).copy_from(sha1sha1.to_unsafe, 20)

        sizet_40 = LibC::SizeT.new(40)
        buffer_sha1 = OpenSSL::SHA1.hash(buffer.to_unsafe, sizet_40)

        # reuse buffer
        20.times { |i|
          buffer[i] = sha1[i] ^ buffer_sha1[i]
        }

        auth_response = Bytes.new(buffer.to_unsafe, 20)

        # packet.write_byte 0_u8
        packet.write_lenenc_int 20
        packet.write(auth_response)
      else
        packet.write_byte 0_u8
      end

      if initial_catalog = @initial_catalog
        packet << initial_catalog
        packet.write_byte 0_u8
      end

      if @password
        packet << "mysql_native_password"
        packet.write_byte 0_u8
      end
    end
  end
end
