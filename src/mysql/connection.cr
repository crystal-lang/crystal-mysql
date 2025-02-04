require "socket"
require "openssl"

class MySql::Connection < DB::Connection
  enum SSLMode
    Disabled
    Preferred
    Required
    VerifyCA
    VerifyIdentity
  end
  record SSLOptions, mode : SSLMode, key : Path?, cert : Path?, ca : Path? do
    def self.from_params(params : URI::Params)
      mode =
        case (params["ssl-mode"]?).try &.downcase
        when nil
          SSLMode::Preferred
        when "preferred"
          SSLMode::Preferred
        when "disabled"
          SSLMode::Disabled
        when "required"
          SSLMode::Required
        when "verifyca", "verify-ca", "verify_ca"
          SSLMode::VerifyCA
        when "verifyidentity", "verify-identity", "verify_identity"
          SSLMode::VerifyIdentity
        else
          raise ArgumentError.new(%(invalid "#{params["ssl-mode"]}" value for ssl-mode))
        end

      # NOTE: Passing paths prefixed with ~/ or ./ seems to not work with OpenSSL
      # we we expand the provided path.
      key = (params["ssl-key"]?).try { |v| Path[File.expand_path(v, home: true)] }
      cert = (params["ssl-cert"]?).try { |v| Path[File.expand_path(v, home: true)] }
      ca = (params["ssl-ca"]?).try { |v| Path[File.expand_path(v, home: true)] }

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

      handshake_response = Protocol::HandshakeResponse41.new(mysql_options.username, mysql_options.password, mysql_options.initial_catalog, handshake.auth_plugin_data, charset_id)
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

      write_packet(seq) do |packet|
        handshake_response.write(packet)
      end

      read_ok_or_err do |packet, status|
        raise "packet #{status} not implemented"
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
    8.times { packet.read_byte! }
    raise packet.read_string
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
