require "socket"

class MySql::Connection < DB::Connection
  def initialize(db : DB::Database)
    super(db)
    @socket = uninitialized TCPSocket

    begin
      host = db.uri.host.not_nil!
      port = db.uri.port || 3306
      username = db.uri.user
      password = db.uri.password

      path = db.uri.path
      if path && path.size > 1
        initial_catalog = path[1..-1]
      else
        initial_catalog = nil
      end

      @socket = TCPSocket.new(host, port)
      handshake = read_packet(Protocol::HandshakeV10)

      write_packet(1) do |packet|
        Protocol::HandshakeResponse41.new(username, password, initial_catalog, handshake.auth_plugin_data).write(packet)
      end

      read_ok_or_err do |packet, status|
        raise "packet #{status} not implemented"
      end
    rescue Errno
      raise DB::ConnectionRefused.new
    end
  end

  # :nodoc:
  def read_ok_or_err
    read_packet do |packet|
      raise_if_err_packet(packet) do |status|
        yield packet, status
      end
    end
  end

  # :nodoc:
  def read_packet
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
  def write_packet(seq = 0)
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
  def raise_if_err_packet(packet)
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
        character_set = packet.read_fixed_int(2).to_u16
        column_length = packet.read_fixed_int(4).to_u32
        column_type = packet.read_fixed_int(1).to_u8
        flags = packet.read_fixed_int(2).to_u16
        decimal = packet.read_fixed_int(1).to_u8
        filler = packet.read_fixed_int(2).to_u16 # filler [00] [00]
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

  def build_prepared_statement(query)
    MySql::Statement.new(self, query)
  end

  def build_unprepared_statement(query)
    MySql::UnpreparedStatement.new(self, query)
  end
end
