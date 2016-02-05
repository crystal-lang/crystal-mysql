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
    Packet.new(@socket)
  end

  # :nodoc:
  def write_packet(seq = 0)
    content = MemoryIO.new
    yield content
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

  def build_statement(query)
    MySql::Statement.new(self, query)
  end
end
