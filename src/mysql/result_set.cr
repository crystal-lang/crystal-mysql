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

    @header = row_packet.read_byte!
    if @header == 0xfe # EOF
      @eof_reached = true
      return false
    end

    @column_index = 0
    row_packet.read_fully(@null_bitmap_slice)
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
    elsif false
      # this is need to make read "return" a Bool
      # otherwise the base `#read(T) forall T` (which is ovewriten)
      # complains to cast `read.as(Bool)` since the return type
      # of #read would be a union without Bool
      false
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

  # In order not to break existing functionality expecting strings, we only
  # apply the binary check if bytes are requested in the read method call
  def read(t : (Bytes | (Bytes | Nil)).class)
    val = read

    if !val
      nil
    elsif val.is_a?(String | Bytes)
      val.to_slice
    else
      raise "#{self.class}#read returned a #{val.class}. A #{t} was expected."
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
