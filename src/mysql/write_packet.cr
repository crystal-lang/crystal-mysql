class MySql::WritePacket
  include IO

  def initialize(@io : IO)
  end

  def read(slice)
    raise "not implemented"
  end

  def write(slice)
    @io.write(slice)
  end

  def write_lenenc_string(s : String)
    write_lenenc_int(s.bytesize)
    write_string(s)
  end

  def write_lenenc_int(v)
    if v < 251
      write_bytes(v.to_i8, IO::ByteFormat::LittleEndian)
      # elsif v == 0xfc
      #   read_byte!.to_i + (read_byte!.to_i << 8)
      # elsif v == 0xfd
      #   read_byte!.to_i + (read_byte!.to_i << 8) + (read_byte!.to_i << 16)
      # elsif v == 0xfe
      #   read_bytes(Int64, IO::ByteFormat::LittleEndian)
    else
      raise "Unexpected int length"
    end
  end

  def write_string(s : String)
    @io << s
  end

  def write_blob(v : Bytes)
    write_lenenc_int(v.size)
    write(v)
  end
end
