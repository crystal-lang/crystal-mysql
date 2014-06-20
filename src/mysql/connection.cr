require "socket"

class MySql::Connection
  def initialize(host, port, username, password)
    @socket = BufferedIO.new(TCPSocket.new(host, port))
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

      packet.write [0x00_u8, 0x00_u8, 0x00_u8, 0x00_u8]
      packet.write_byte 0x00_u8
      23.times { packet.write_byte 0x00_u8 }
      packet.write username.cstr, username.length + 1
      packet.write_byte 00_u8
      # 20.times { packet.write_byte 0x00_u8 }
    end

    read_packet do |packet|
      if packet.read_byte == 255
        4.times { packet.read_byte }
        raise packet.read_string
      end
    end
  end

  def read_packet
    packet = Packet.new(@socket)
    begin
      yield packet
    ensure
      packet.discard
    end
  end

  def write_packet(seq = 0)
    content = StringIO.new
    yield content
    length = content.buffer.length
    packet = StringIO.new
    3.times do
      packet.write_byte (length & 0xff_u8).to_u8
      length >>= 8
    end
    packet.write_byte seq.to_u8

    packet.write content.buffer.buffer, content.buffer.length

    @socket.write packet.buffer.buffer, packet.buffer.length
    @socket.flush
  end

  def handle_err_packet(packet)
    8.times { packet.read_byte! }
    raise packet.read_string
  end

  def execute(sql)
    write_packet do |packet|
      packet.write_byte 0x03u8
      packet.write sql.cstr, sql.length
    end

    read_packet do |packet|
      case header = packet.read_byte.not_nil!
      when 255
        handle_err_packet(packet)
      when 0
        Result.new(packet)
      else
        ResultSet.new(self, packet.read_lenenc_int(header))
      end
    end
  end
end
