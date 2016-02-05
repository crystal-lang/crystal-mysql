class MySql::ResultSet < DB::ResultSet
  record ColumnSpec, catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type

  getter columns

  def initialize(statement, column_count)
    super(statement)
    @conn = statement.connection

    @columns = [] of ColumnSpec

    # Parse column definitions
    # http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
    column_count.times do
      @conn.read_packet do |packet|
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

        @columns << ColumnSpec.new(catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type)
      end
    end

    @conn.read_packet do |eof_packet|
      eof_packet.read_byte
    end

    @column_index = 0 # next column index to return
    @header = 0
  end

  def do_close
    super

    while move_next
    end

    if row_packet = @row_packet
      row_packet.discard
    end
  end

  def move_next : Bool
    # TODO skip all remaining cols in the current row
    # TODO check if a row can be skipped without reading any column
    if row_packet = @row_packet
      row_packet.discard
    end

    @row_packet = row_packet = @conn.build_read_packet

    @header = row_packet.read_byte!
    return false if @header == 0xfe # EOF
    @column_index = 0
    return true
  end

  def column_count : Int32
    @columns.size
  end

  def column_name(index : Int32) : String
    @columns[index].name
  end

  def column_type(index : Int32)
    # Column types
    # http://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
    case @columns[index].column_type
    when 0x03; Int32
    when 0x08; Int64
    when 0xfc; 0xfb; Slice(UInt8)
    else       String
    end
  end

  def read_if_not_nil
    row_packet = @row_packet.not_nil!

    header =
      if @column_index > 0
        row_packet.read_byte!
      else
        @header
      end
    @column_index += 1
    if header == 0xfb
      nil
    else
      length = row_packet.read_lenenc_int(header)
      yield row_packet, length
    end
  end

  def read?(t : String.class) : String?
    read_if_not_nil do |row_packet, length|
      row_packet.read_string(length)
    end
  end

  def read?(t : Int32.class) : Int32?
    read_if_not_nil do |row_packet, length|
      row_packet.read_int_string(length)
    end
  end

  def read?(t : Int64.class) : Int64?
    read_if_not_nil do |row_packet, length|
      row_packet.read_int64_string(length)
    end
  end

  def read?(t : Float32.class) : Float32?
    raise "not implemented"
  end

  def read?(t : Float64.class) : Float64?
    raise "not implemented"
  end

  def read?(t : Slice(UInt8).class) : Slice(UInt8)?
    read_if_not_nil do |row_packet, length|
      ary = row_packet.read_byte_array(length.to_i32)
      Slice.new(ary.to_unsafe, ary.size)
    end
  end
end
