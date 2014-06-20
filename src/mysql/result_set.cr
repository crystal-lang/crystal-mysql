class MySql::ResultSet
  def initialize(@conn, @columns)
    @columns.times do
      @conn.read_packet do |packet|
      end
    end

    @conn.read_packet do |eof_packet|
      eof_packet.read_byte
    end
  end

  def each_row
    eof = false
    while !eof
      @conn.read_packet do |row_packet|
        row = [] of String?
        @columns.times do
          header = row_packet.read_byte!
          case header
          when 0xfe
            return
          when 0xfb
            row << nil
          else
            row << row_packet.read_string(row_packet.read_lenenc_int(header))
          end
        end
        yield row
      end
    end
  end
end
