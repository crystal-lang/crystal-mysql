class MySql::Statement < DB::Statement
  def initialize(connection, @sql)
    super(connection)
  end

  protected def perform_query(args : Slice(DB::Any)) : DB::ResultSet
    perform_exec_or_query(args) as DB::ResultSet
  end

  protected def perform_exec(args : Slice(DB::Any)) : DB::ExecResult
    perform_exec_or_query(args) as DB::ExecResult
  end

  private def perform_exec_or_query(args : Slice(DB::Any))
    @connection.write_packet do |packet|
      packet.write_byte 0x03u8
      packet << @sql
    end

    @connection.read_packet do |packet|
      case header = packet.read_byte.not_nil!
      when 255
        @connection.handle_err_packet(packet)
      when 0
        affected_rows = packet.read_lenenc_int
        last_insert_id = packet.read_lenenc_int
        DB::ExecResult.new affected_rows, last_insert_id
      else
        MySql::ResultSet.new(self, packet.read_lenenc_int(header))
      end
    end
  end
end
