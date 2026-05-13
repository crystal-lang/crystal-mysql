require "socket"
require "openssl"
require "./auth"

class MySql::Connection < DB::Connection
  class PacketError < Exception; end

  enum SSLMode
    Disabled
    Preferred
    Required
    VerifyCA
    VerifyIdentity
  end
  record SSLOptions, mode : SSLMode, key : Path? = nil, cert : Path? = nil, ca : Path? = nil do
    def self.from_params(params : URI::Params)
      mode =
        if mode_param = params["ssl-mode"]?
          SSLMode.parse(mode_param)
        else
          SSLMode::Preferred
        end

      # NOTE: Passing paths prefixed with ~/ or ./ seems to not work with OpenSSL
      # we we expand the provided path.
      key = (params["ssl-key"]?).try { |v| Path[v].expand(home: true) }
      cert = (params["ssl-cert"]?).try { |v| Path[v].expand(home: true) }
      ca = (params["ssl-ca"]?).try { |v| Path[v].expand(home: true) }

      SSLOptions.new(mode: mode, key: key, cert: cert, ca: ca)
    end

    def build_context : OpenSSL::SSL::Context::Client
      ctx = OpenSSL::SSL::Context::Client.new

      ctx.verify_mode =
        case mode
        when SSLMode::VerifyCA, SSLMode::VerifyIdentity
          OpenSSL::SSL::VerifyMode::PEER
        else
          OpenSSL::SSL::VerifyMode::NONE
        end

      ctx.certificate_chain = cert.to_s if cert = @cert
      ctx.private_key = key.to_s if key = @key
      ctx.ca_certificates = ca.to_s if ca = @ca

      ctx
    end
  end

  record Options,
    transport : URI,
    username : String?,
    password : String?,
    initial_catalog : String?,
    charset : String,
    ssl_options : SSLOptions do
    def self.from_uri(uri : URI) : Options
      params = uri.query_params
      initial_catalog = params["database"]?

      if (host = uri.hostname) && !host.blank?
        port = uri.port || 3306
        transport = URI.new("tcp", host, port)

        # for tcp socket we support the first component to be the database
        # but the query string takes precedence because it's more explicit
        if initial_catalog.nil? && (path = uri.path) && path.size > 1
          initial_catalog = path[1..-1]
        end
      else
        transport = URI.new("unix", nil, nil, uri.path)
      end

      username = uri.user
      password = uri.password

      charset = params.fetch "encoding", Collations.default_collation

      Options.new(
        transport: transport,
        username: username, password: password,
        initial_catalog: initial_catalog, charset: charset,
        ssl_options: SSLOptions.from_params(params)
      )
    end
  end

  def initialize(options : ::DB::Connection::Options, mysql_options : ::MySql::Connection::Options)
    super(options)
    @socket = uninitialized UNIXSocket | TCPSocket | OpenSSL::SSL::Socket::Client

    begin
      charset_id = Collations.id_for_collation(mysql_options.charset).to_u8

      transport = mysql_options.transport
      hostname = nil
      @socket =
        case transport.scheme
        when "tcp"
          hostname = transport.host || raise "Missing host in transport #{transport}"
          TCPSocket.new(hostname, transport.port)
        when "unix"
          UNIXSocket.new(transport.path)
        else
          raise "Transport not supported #{transport}"
        end

      handshake = read_packet(Protocol::HandshakeV10)

      handshake_response = Protocol::HandshakeResponse41.new(
        mysql_options.username, mysql_options.password, mysql_options.initial_catalog,
        handshake.auth_plugin_data, charset_id, handshake.server_plugin_name)
      seq = 1

      if mysql_options.ssl_options.mode != SSLMode::Disabled &&
         # socket connection will not use ssl for preferred
         !(transport.scheme == "unix" && mysql_options.ssl_options.mode == SSLMode::Preferred)
        write_packet(seq) do |packet|
          handshake_response.write_ssl_request(packet)
        end
        seq += 1
        ctx = mysql_options.ssl_options.build_context
        @socket = OpenSSL::SSL::Socket::Client.new(@socket, context: ctx, sync_close: true, hostname: hostname)
        # NOTE: If ssl_options.mode is Preferred we should fallback to non-ssl socket if the ssl setup failed
        # if we do so, we should warn at least. Making Preferred behave as Required is a safer option
        # so the user would need to explicitly choose Disabled to avoid the ssl setup.
      end

      ssl_established = @socket.is_a?(OpenSSL::SSL::Socket::Client)

      write_packet(seq) do |packet|
        handshake_response.write(packet, ssl_established)
      end
      seq += 1

      # Auth state machine
      plugin_name = handshake.server_plugin_name
      scramble = handshake.auth_plugin_data

      auth_complete = false
      until auth_complete
        read_packet do |packet|
          seq = packet.seq.to_i32 + 1
          status = packet.read_byte!

          case status
          when 0x00
            # OK packet — authentication successful
            auth_complete = true
          when 0xFF
            # ERR packet
            handle_err_packet(packet)
          when 0xFE
            # AuthSwitchRequest: 0xFE, plugin_name\0, plugin_data\0
            plugin_name = packet.read_string
            # Scramble is up to 20 random bytes (may contain 0x00 internally) followed
            # by a trailing null. Bound to remaining-1 so the null stays for discard.
            scramble_size = {20, packet.remaining - 1}.min
            scramble_size = 0 if scramble_size < 0
            new_scramble = Bytes.new(scramble_size)
            packet.read_fully(new_scramble) if scramble_size > 0
            scramble = new_scramble if scramble_size > 0

            auth_response = Auth.compute_auth_response(plugin_name, mysql_options.password, scramble, ssl_established)
            write_packet(seq) do |pkt|
              pkt.write(auth_response)
            end
            seq += 1
          when 0x01
            # AuthMoreData
            seq = handle_auth_more_data(packet, plugin_name, mysql_options.password, scramble, ssl_established, seq)
          else
            raise PacketError.new("Unexpected auth packet status: #{status}")
          end
        end
      end
    rescue IO::Error
      raise DB::ConnectionRefused.new
    end
  end

  def do_close
    super

    begin
      write_packet do |packet|
        Protocol::Quit.new.write(packet)
      end
      @socket.close
    rescue
    end
  end

  # :nodoc:
  def read_ok_or_err(&)
    read_packet do |packet|
      raise_if_err_packet(packet) do |status|
        yield packet, status
      end
    end
  end

  # :nodoc:
  def read_packet(&)
    packet = build_read_packet
    begin
      yield packet
    ensure
      packet.discard
    end
  end

  # :nodoc:
  def read_packet(protocol_packet_type)
    read_packet do |packet|
      return protocol_packet_type.read(packet)
    end
    raise "unable to read packet"
  end

  # :nodoc:
  def build_read_packet
    ReadPacket.new(@socket, self)
  end

  # :nodoc:
  def write_packet(seq = 0, &)
    content = IO::Memory.new
    yield WritePacket.new(content, self)
    bytesize = content.bytesize

    packet = IO::Memory.new
    3.times do
      packet.write_byte (bytesize & 0xff_u8).to_u8
      bytesize >>= 8
    end
    packet.write_byte seq.to_u8

    packet << content

    @socket << packet
    @socket.flush
  end

  # :nodoc:
  def handle_err_packet(packet)
    error_code = packet.read_fixed_int(2)
    packet.read_byte_array(6)
    message = packet.read_string

    # https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
    # https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html
    # Error 1053: Server shutdown in progress
    # Error 1152: Aborted connection to db user
    # Error 1927: Connection was killed
    # Error 2006: MySQL server has gone away
    # Error 2013: Lost connection to MySQL server during query
    case error_code
    when 1053, 1152, 1927, 2006, 2013
      raise DB::ConnectionLost.new(self, PacketError.new(message))
    else
      raise PacketError.new(message)
    end
  end

  private def handle_auth_more_data(packet : ReadPacket, plugin_name : String, password : String?, scramble : Bytes, ssl_established : Bool, seq : Int32) : Int32
    case plugin_name
    when "caching_sha2_password"
      flag = packet.read_byte!
      case flag
      when 0x03
        # Fast auth success — next packet will be OK
      when 0x04
        # Full authentication required
        if ssl_established
          # Over TLS: send plaintext password null-terminated
          pw = (password || "").to_slice
          write_packet(seq) do |pkt|
            pkt.write(pw)
            pkt.write_byte(0_u8)
          end
          seq += 1
        else
          # Request RSA public key
          write_packet(seq) do |pkt|
            pkt.write_byte(0x02_u8)
          end
          seq += 1

          # Read RSA public key
          read_packet do |key_packet|
            seq = key_packet.seq.to_i32 + 1
            key_status = key_packet.read_byte!
            raise PacketError.new("Expected AuthMoreData with RSA key, got #{key_status}") unless key_status == 0x01
            pem_data = key_packet.read_string(key_packet.remaining)
            encrypted = Auth.rsa_encrypt_password(password || "", scramble, pem_data)
            write_packet(seq) do |pkt|
              pkt.write(encrypted)
            end
            seq += 1
          end
        end
      else
        raise PacketError.new("Unexpected caching_sha2_password flag: #{flag}")
      end
    when "sha256_password"
      pem_data = packet.read_string(packet.remaining)
      if ssl_established
        # Over TLS: send plaintext password null-terminated
        pw = (password || "").to_slice
        write_packet(seq) do |pkt|
          pkt.write(pw)
          pkt.write_byte(0_u8)
        end
        seq += 1
      else
        encrypted = Auth.rsa_encrypt_password(password || "", scramble, pem_data)
        write_packet(seq) do |pkt|
          pkt.write(encrypted)
        end
        seq += 1
      end
    else
      raise PacketError.new("AuthMoreData not expected for plugin: #{plugin_name}")
    end
    seq
  end

  # :nodoc:
  def raise_if_err_packet(packet)
    raise_if_err_packet(packet) do |status|
      raise "unexpected packet #{status}"
    end
  end

  # :nodoc:
  def raise_if_err_packet(packet, &)
    status = packet.read_byte!
    if status == 255
      handle_err_packet packet
    end

    yield status if status != 0

    status
  end

  # :nodoc:
  def read_column_definitions(target, column_count)
    # Parse column definitions
    # http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
    column_count.times do
      self.read_packet do |packet|
        catalog = packet.read_lenenc_string
        schema = packet.read_lenenc_string
        table = packet.read_lenenc_string
        org_table = packet.read_lenenc_string
        name = packet.read_lenenc_string
        org_name = packet.read_lenenc_string
        next_length = packet.read_lenenc_int # length of fixed-length fields, always 0x0c
        raise "Unexpected next_length value: #{next_length}." unless next_length == 0x0c
        character_set = packet.read_fixed_int(2).to_u16!
        column_length = packet.read_fixed_int(4).to_u32!
        column_type = packet.read_fixed_int(1).to_u8!
        flags = packet.read_fixed_int(2).to_u16!
        decimal = packet.read_fixed_int(1).to_u8!
        filler = packet.read_fixed_int(2).to_u16! # filler [00] [00]
        raise "Unexpected filler value #{filler}" unless filler == 0x0000

        target << ColumnSpec.new(catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type, flags, decimal)
      end
    end

    if column_count > 0
      self.read_packet do |eof_packet|
        eof_packet.read_byte # TODO assert EOF Packet
      end
    end
  end

  def build_prepared_statement(query) : MySql::Statement
    MySql::Statement.new(self, query)
  end

  def build_unprepared_statement(query) : MySql::UnpreparedStatement
    MySql::UnpreparedStatement.new(self, query)
  end
end
