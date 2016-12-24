class MySql::ReadPacket
  include IO

  @length : Int32 = 0
  @remaining : Int32 = 0
  @seq : UInt8 = 0u8

  def initialize(@io : IO, @connection : Connection)
    begin
      header = uninitialized UInt8[4]
      io.read_fully(header.to_slice)
      @length = @remaining = header[0].to_i + (header[1].to_i << 8) + (header[2].to_i << 16)
      @seq = header[3]
    rescue IO::EOFError
      raise DB::ConnectionLost.new(@connection)
    end
  end

  def to_s(io)
    io << "MySql::IncomingPacket[length: " << @length << ", seq: " << @seq << ", remaining: " << @remaining << "]"
  end

  def read(slice : Bytes)
    return 0 unless @remaining > 0
    read_bytes = @io.read(slice)
    @remaining -= read_bytes
    read_bytes
  rescue IO::EOFError
    raise DB::ConnectionLost.new(@connection)
  end

  def write(slice)
    raise "not implemented"
  end

  def read_byte!
    read_byte || raise "Unexpected EOF"
  end

  def read_string
    String.build do |buffer|
      while (b = read_byte) != 0 && b
        buffer.write_byte b if b
      end
    end
  end

  def read_string(length)
    String.build do |buffer|
      length.to_i64.times do
        buffer.write_byte read_byte!
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

  # TODO: should return different types of int depending on n value (note: update Connection#read_column_definitions to remote to_i16/to_i8)
  def read_fixed_int(n)
    int = 0
    n.times do |i|
      int += (read_byte!.to_i << (i * 8))
    end
    int
  end

  def read_lenenc_int(h = read_byte!)
    res = if h < 251
            h.to_i
          elsif h == 0xfc
            read_byte!.to_i + (read_byte!.to_i << 8)
          elsif h == 0xfd
            read_byte!.to_i + (read_byte!.to_i << 8) + (read_byte!.to_i << 16)
          elsif h == 0xfe
            read_bytes(Int64, IO::ByteFormat::LittleEndian)
          else
            raise "Unexpected int length"
          end

    res.to_i64
  end

  def read_byte_array(length)
    Array(UInt8).new(length) { |i| read_byte! }
  end

  def read_blob
    ary = read_byte_array(read_lenenc_int.to_i32)
    Bytes.new(ary.to_unsafe, ary.size)
  end

  def read_int_string(length)
    value = 0
    length.times do
      value = value * 10 + read_byte!.chr.to_i
    end
    value
  end

  def read_int64_string(length)
    value = 0i64
    length.times do
      value = value * 10i64 + read_byte!.chr.to_i.to_i64
    end
    value
  end

  def discard
    skip(@remaining) if @remaining > 0
  end
end
