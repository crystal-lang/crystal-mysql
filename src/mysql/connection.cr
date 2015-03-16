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

      28.times { packet.write_byte 0_u8 }
      packet << username
      2.times { packet.write_byte 0_u8 }
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
    bytesize = content.bytesize

    packet = StringIO.new
    3.times do
      packet.write_byte (bytesize & 0xff_u8).to_u8
      bytesize >>= 8
    end
    packet.write_byte seq.to_u8

    packet << content

    @socket << packet
    @socket.flush
  end

  def handle_err_packet(packet)
    8.times { packet.read_byte! }
    raise packet.read_string
  end

  def execute(sql)
    write_packet do |packet|
      packet.write_byte 0x03u8
      packet << sql
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
