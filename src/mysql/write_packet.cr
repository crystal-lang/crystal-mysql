class MySql::WritePacket
  include IO

  def initialize(@io : IO, @connection : Connection)
  end

  def read(slice)
    raise "not implemented"
  end

  def write(slice)
    @io.write(slice)
  rescue IO::EOFError
    raise DB::ConnectionLost.new(@connection)
  end

  def write_lenenc_string(s : String)
    write_lenenc_int(s.bytesize)
    write_string(s)
  end

  def write_lenenc_int(v)
    if v < 251
      write_bytes(v.to_i8, IO::ByteFormat::LittleEndian)
    elsif v < 65_536
      write_bytes(0xfc_u8, IO::ByteFormat::LittleEndian)
      write_bytes(v.to_u16, IO::ByteFormat::LittleEndian)
    elsif v < 16_777_216
      write_bytes(0xfd_u8, IO::ByteFormat::LittleEndian)
      write_bytes((v & 0x000000FF).to_u8)
      write_bytes(((v & 0x0000FF00) >> 8).to_u8)
      write_bytes(((v & 0x00FF0000) >> 16).to_u8)
    else
      write_bytes(0xfe_u8, IO::ByteFormat::LittleEndian)
      write_bytes(v.to_u64, IO::ByteFormat::LittleEndian)
    end
  end

  def write_string(s : String)
    @io << s
  rescue IO::EOFError
    raise DB::ConnectionLost.new(@connection)
  end

  def write_blob(v : Bytes)
    write_lenenc_int(v.size)
    write(v)
  end
end
