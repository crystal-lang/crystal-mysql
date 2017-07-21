# Implementation of ProtocolText::Resultset.
# Used for unprepared statements.
class MySql::TextResultSet < DB::ResultSet
  getter columns

  @conn : MySql::Connection
  @row_packet : MySql::ReadPacket?
  @first_byte : UInt8

  def initialize(statement, column_count)
    super(statement)
    @conn = statement.connection.as(MySql::Connection)

    columns = @columns = [] of ColumnSpec
    @conn.read_column_definitions(columns, column_count)

    @column_index = 0 # next column index to return

    @first_byte = 0u8
    @eof_reached = false
    @first_row_packet = false
  end

  def do_close
    while move_next
    end

    if row_packet = @row_packet
      row_packet.discard
    end
  ensure
    super
  end

  def move_next : Bool
    return false if @eof_reached

    # skip previous row_packet
    if row_packet = @row_packet
      row_packet.discard
    end

    @row_packet = row_packet = @conn.build_read_packet

    @first_byte = row_packet.read_byte!
    if @first_byte == 0xfe # EOF
      @eof_reached = true
      return false
    end

    @column_index = 0
    @first_row_packet = true
    # TODO remove row_packet.read(@null_bitmap_slice)
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

    if @first_row_packet
      current_byte = @first_byte
      @first_row_packet = false
    else
      current_byte = row_packet.read_byte!
    end

    is_nil = current_byte == 0xfb
    col = @column_index
    @column_index += 1
    if is_nil
      nil
    else
      length = row_packet.read_lenenc_int(current_byte)
      val = row_packet.read_string(length)
      val = @columns[col].column_type.parse(val)

      # http://dev.mysql.com/doc/internals/en/character-set.html
      if val.is_a?(Slice(UInt8)) && @columns[col].character_set != 63
        ::String.new(val)
      else
        val
      end
    end
  end

  def read(t : Bool.class)
    MySql::Type.from_mysql(read.as(Int8))
  end

  def read(t : (Bool | Nil).class)
    if v = read.as(Int8 | Nil)
      MySql::Type.from_mysql(v)
    else
      nil
    end
  end
end
