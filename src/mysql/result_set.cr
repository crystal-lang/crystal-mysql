require "bit_array"

class MySql::ResultSet < DB::ResultSet
  getter columns

  @conn : MySql::Connection
  @row_packet : MySql::ReadPacket?
  @header : UInt8
  @null_bitmap_slice : Bytes

  def initialize(statement, column_count)
    super(statement)
    @conn = statement.connection.as(MySql::Connection)

    columns = @columns = [] of ColumnSpec
    @conn.read_column_definitions(columns, column_count)

    @column_index = 0 # next column index to return
    @null_bitmap = BitArray.new(columns.size + 2)
    @null_bitmap_slice = @null_bitmap.to_slice

    @header = 0u8
    @eof_reached = false
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
    return false if @eof_reached

    # skip previous row_packet
    if row_packet = @row_packet
      row_packet.discard
    end

    @row_packet = row_packet = @conn.build_read_packet

    @header = row_packet.read_byte!
    if @header == 0xfe # EOF
      @eof_reached = true
      return false
    end

    @column_index = 0
    row_packet.read(@null_bitmap_slice)
    return true
  end

  def column_count : Int32
    @columns.size
  end

  def column_name(index : Int32) : String
    @columns[index].name
  end

  def read
    row_packet = @row_packet.not_nil!

    is_nil = @null_bitmap[@column_index + 2]
    col = @column_index
    @column_index += 1
    if is_nil
      nil
    else
      val = @columns[col].column_type.read(row_packet)
      # http://dev.mysql.com/doc/internals/en/character-set.html
      if val.is_a?(Slice(UInt8)) && @columns[col].character_set != 63
        ::String.new(val)
      else
        val
      end
    end
  end
end
