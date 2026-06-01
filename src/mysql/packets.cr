require "openssl/sha1"

module MySql
  # https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
  @[Flags]
  enum Capability : UInt32
    LongPassword               = 0x00000001
    FoundRows                  = 0x00000002
    LongFlag                   = 0x00000004
    ConnectWithDB              = 0x00000008
    NoSchema                   = 0x00000010
    Compress                   = 0x00000020
    ODBC                       = 0x00000040
    LocalFiles                 = 0x00000080
    IgnoreSpace                = 0x00000100
    Protocol41                 = 0x00000200
    Interactive                = 0x00000400
    SSL                        = 0x00000800
    IgnoreSigpipe              = 0x00001000
    Transactions               = 0x00002000
    Reserved                   = 0x00004000
    SecureConnection           = 0x00008000
    MultiStatements            = 0x00010000
    MultiResults               = 0x00020000
    PSMultiResults             = 0x00040000
    PluginAuth                 = 0x00080000
    ConnectAttrs               = 0x00100000
    PluginAuthLenencClientData = 0x00200000
    CanHandleExpiredPasswords  = 0x00400000
    SessionTrack               = 0x00800000
    DeprecateEOF               = 0x01000000
  end
end

module MySql::Protocol
  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
  struct HandshakeV10
    getter auth_plugin_data : Bytes
    getter charset : UInt8
    getter server_capabilities : Capability
    getter server_plugin_name : String

    def initialize(@auth_plugin_data, @charset, @server_capabilities, @server_plugin_name)
    end

    def self.read(packet : MySql::ReadPacket)
      protocol_version = packet.read_byte!
      version = packet.read_string
      thread = packet.read_int

      auth_data = Bytes.new(20)
      packet.read_fully(auth_data[0, 8])
      packet.read_byte!
      cap1 = packet.read_byte!
      cap2 = packet.read_byte!
      charset = packet.read_byte!
      packet.read_byte_array(2)
      cap3 = packet.read_byte!
      cap4 = packet.read_byte!

      server_capabilities = Capability.new(cap1.to_u32 | (cap2.to_u32 << 8) | (cap3.to_u32 << 16) | (cap4.to_u32 << 24))

      auth_plugin_data_length = packet.read_byte!
      packet.read_byte_array(10)
      packet.read_fully(auth_data[8, {13, auth_plugin_data_length.to_i16 - 8}.max - 1])
      packet.read_byte!
      server_plugin_name = packet.read_string

      HandshakeV10.new(auth_data, charset, server_capabilities, server_plugin_name)
    end
  end

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41
  struct HandshakeResponse41
    def initialize(@username : String?, @password : String?, @initial_catalog : String?, @auth_plugin_data : Bytes, @charset : UInt8, @plugin_name : String = "mysql_native_password")
    end

    # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html
    def write_ssl_request(packet : MySql::WritePacket)
      caps = Capability::Protocol41 | Capability::SSL
      caps |= Capability::ConnectWithDB if @initial_catalog

      packet.write_bytes caps.value, IO::ByteFormat::LittleEndian

      packet.write_bytes 0x00000000u32, IO::ByteFormat::LittleEndian
      packet.write_byte @charset
      23.times { packet.write_byte 0_u8 }
    end

    def write(packet : MySql::WritePacket)
      caps = Capability::Protocol41 | Capability::SecureConnection | Capability::PluginAuthLenencClientData
      caps |= Capability::ConnectWithDB if @initial_catalog

      packet.write_bytes caps.value, IO::ByteFormat::LittleEndian
      packet.write_bytes 0x00000000u32, IO::ByteFormat::LittleEndian
      packet.write_byte @charset
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

  struct Quit
    def write(packet : MySql::WritePacket)
      packet.write_byte 1_u8
    end
  end
end
