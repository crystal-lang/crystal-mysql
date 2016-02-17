require "bit_array"

struct BitArray
  getter bits
end

class MySql::ResultSet < DB::ResultSet
  getter columns

  def initialize(statement, column_count)
    super(statement)
    @conn = statement.connection

    columns = @columns = [] of ColumnSpec
    @conn.read_column_definitions(columns, column_count)

    @column_index = 0 # next column index to return

    @null_bitmap = BitArray.new(columns.size + 7 + 2)
    @null_bitmap_slice = Slice.new(@null_bitmap.bits as Pointer(UInt8), (columns.size + 7 + 2) / 8)

    @header = 0
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

  def column_type(index : Int32)
    @columns[index].column_type.db_any_type
  end

  def read_if_not_nil
    row_packet = @row_packet.not_nil!

    is_nil = @null_bitmap[@column_index + 2]
    col = @column_index
    @column_index += 1
    if is_nil
      nil
    else
      yield row_packet, col
    end
  end

  {% for t in DB::TYPES %}
    def read?(t : {{t}}.class) : {{t}}?
      read_if_not_nil do |row_packet, col|
        @columns[col].column_type.read(row_packet) as {{t}}
      end
    end
  {% end %}
end
