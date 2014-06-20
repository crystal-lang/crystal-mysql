module IO
  def read_fully(buffer : UInt8*, count)
    while count > 0
      read_bytes = read(buffer, count)
      raise "EOF" if read_bytes == 0
      count -= read_bytes
      buffer += read_bytes
    end
    count
  end
end

class MySql::Packet
  include IO

  def initialize(@io)
    header :: UInt32
    header_ptr = pointerof(header) as UInt8*
    io.read_fully(header_ptr, 4)
    @length = @remaining = header_ptr[0].to_i + (header_ptr[1].to_i << 8) + (header_ptr[2].to_i << 16)
    @seq = header_ptr[3]
  end

  def to_s
    "MySql::Packet[length: #{@length}, seq: #{@seq}, remaining: #{@remaining}]"
  end

  def read(buffer : UInt8*, count)
    return 0 unless @remaining > 0
    read_bytes = @io.read(buffer, count)
    @remaining -= read_bytes
    read_bytes
  end

  def read_byte!
    read_byte || raise "Unexpected EOF"
  end

  def read_string
    String.new_from_buffer do |buffer|
      while (b = read_byte) != 0 && b
        buffer.append_byte b if b
      end
    end
  end

  def read_string(length)
    String.new_from_buffer do |buffer|
      length.times do
        buffer.append_byte read_byte!
      end
    end
  end

  def read_lenenc_string
    length = read_lenenc_int
    read_string(length)
  end

  def read_int
    read_byte!.to_i + (read_byte!.to_i << 8) + (read_byte!.to_i << 16) + (read_byte!.to_i << 24)
  end

  def read_fixed_int(n)
    int = 0
    n.times do |i|
      int += (read_byte!.to_i << (i * 8))
    end
    int
  end

  def read_lenenc_int(h = read_byte!)
    if h < 251
      h.to_i
    elsif h == 0xfc
      read_byte!.to_i + (read_byte!.to_i << 8)
    elsif h == 0xfd
      read_byte!.to_i + (read_byte!.to_i << 8) + (read_byte!.to_i << 16)
    elsif h == 0xfe
      raise "8 byte int not implemented"
    else
      raise "Unexpected int length"
    end
  end

  def read_byte_array(length)
    Array(UInt8).new(length) { |i| read_byte! }
  end

  def discard
    read(@remaining) if @remaining > 0
  end
end
