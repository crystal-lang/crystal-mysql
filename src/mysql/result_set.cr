class MySql::ResultSet
  make_named_tuple ColumnSpec, [catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type]
  alias ColumnType = Int32 | String | Nil

  def initialize(@conn, column_count)
    @columns = [] of ColumnSpec

    # Parse column definitions
    # http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
    column_count.times do
      @conn.read_packet do |packet|
        catalog = packet.read_lenenc_string
        schema = packet.read_lenenc_string
        table = packet.read_lenenc_string
        org_table = packet.read_lenenc_string
        name = puts packet.read_lenenc_string
        org_name = packet.read_lenenc_string
        character_set = packet.read_lenenc_int
        column_length = packet.read_fixed_int(2)
        packet.read_fixed_int(4) # Skip (length of fixed-length fields, always 0x0c)
        column_type = packet.read_fixed_int(1)

        @columns << ColumnSpec.new(catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type)
      end
    end

    @conn.read_packet do |eof_packet|
      eof_packet.read_byte
    end
  end

  def read_column_value(c, value)
    # Column types
    # http://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
    case c.column_type
    when 3 then value.to_i
    else value
    end
  end

  def each_row
    eof = false
    while !eof
      @conn.read_packet do |row_packet|
        row = [] of ColumnType
        @columns.each do |c|
          header = row_packet.read_byte!
          case header
          when 0xfe
            return
          when 0xfb
            row << nil
          else
            value = row_packet.read_string(row_packet.read_lenenc_int(header))
            row << read_column_value(c, value)
          end
        end
        yield row
      end
    end
  end
end
