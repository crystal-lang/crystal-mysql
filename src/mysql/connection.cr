require "socket"

class MySql::Connection < DB::Connection
  def initialize(db : DB::Database)
    super(db)

    host = db.uri.host.not_nil!
    port = db.uri.port || 3306
    username = db.uri.user
    password = db.uri.password
    # TODO use password

    @socket = TCPSocket.new(host, port)
    read_packet do |packet|
      protocol_version = packet.read_byte!
      version = packet.read_string
      thread = packet.read_int
      packet.read_byte_array(9)
    end

    write_packet(1) do |packet|
      caps = 0x0200 | 0x8000 | 0x00200000
      4.times do
        packet.write_byte (caps & 0xff_u8).to_u8
        caps = (caps >> 8)
      end

      28.times { packet.write_byte 0_u8 }
      packet << username
      2.times { packet.write_byte 0_u8 }
    end

    read_ok_or_err

    path = db.uri.path
    if path && path.size > 1
      # http://dev.mysql.com/doc/internals/en/com-init-db.html
      initial_catalog = path[1..-1]

      write_packet do |packet|
        packet.write_byte 2_u8
        packet << initial_catalog
      end

      read_ok_or_err
    end
  end

  # :nodoc:
  def read_ok_or_err
    read_packet do |packet|
      if packet.read_byte == 255
        4.times { packet.read_byte }
        raise packet.read_string
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
  def build_read_packet
    ReadPacket.new(@socket)
  end

  # :nodoc:
  def write_packet(seq = 0)
    content = MemoryIO.new # TODO refactor to a packet wrapper
    yield WritePacket.new(content)
    bytesize = content.bytesize

    packet = MemoryIO.new
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
        character_set = packet.read_lenenc_int
        column_length = packet.read_fixed_int(2)
        packet.read_fixed_int(4) # Skip (length of fixed-length fields, always 0x0c)
        column_type = packet.read_fixed_int(1)

        target << ColumnSpec.new(catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type)
      end
    end

    if column_count > 0
      self.read_packet do |eof_packet|
        eof_packet.read_byte # TODO assert EOF Packet
      end
    end
  end

  def build_statement(query)
    MySql::Statement.new(self, query)
  end
end
