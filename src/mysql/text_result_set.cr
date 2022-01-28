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

  protected def mysql_read
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
      column = @columns[col]
      length = row_packet.read_lenenc_int(current_byte)
      yield row_packet, column, length
    end
  end

  def next_column_index : Int32
    @column_index
  end

  def read
    mysql_read do |row_packet, column, length|
      val = row_packet.read_string(length)
      val = column.column_type.parse(val)

      # http://dev.mysql.com/doc/internals/en/character-set.html
      if val.is_a?(Slice(UInt8)) && column.character_set != 63
        ::String.new(val)
      else
        val
      end
    end
  end

  def read(t : UUID.class)
    read(UUID?).as(UUID)
  end

  def read(t : (UUID | Nil).class)
    mysql_read do |row_packet, column, length|
      if column.flags.bits_set?(128)
        # Check if binary flag is set
        # https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html#gaf74577f0e38eed5616a090965aeac323
        ary = row_packet.read_byte_array(length)
        val = Bytes.new(ary.to_unsafe, ary.size)

        UUID.new val
      else
        val = row_packet.read_string(length)
        raise ::DB::Error.new("The column #{column.name} of type #{column.column_type} returns a #{val.class} and can't be read as UUID")
      end
    end
  end

  def read(t : Bool.class)
    MySql::Type.from_mysql(read.as(Int::Signed))
  end

  def read(t : (Bool | Nil).class)
    if v = read.as(Int::Signed?)
      MySql::Type.from_mysql(v)
    else
      nil
    end
  end
end
