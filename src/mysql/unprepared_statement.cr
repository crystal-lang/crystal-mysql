class MySql::UnpreparedStatement < DB::Statement
  def initialize(connection, @sql : String)
    super(connection)
  end

  protected def conn
    @connection.as(Connection)
  end

  protected def perform_query(args : Enumerable) : DB::ResultSet
    perform_exec_or_query(args).as(DB::ResultSet)
  end

  protected def perform_exec(args : Enumerable) : DB::ExecResult
    perform_exec_or_query(args).as(DB::ExecResult)
  end

  private def perform_exec_or_query(args : Enumerable)
    raise "exec/query with args is not supported" if args.size > 0

    conn = self.conn
    conn.write_packet do |packet|
      packet.write_byte 0x03u8
      packet << @sql
      # TODO to support args an interpolation needs to be done
    end

    conn.read_packet do |packet|
      case header = packet.read_byte.not_nil!
      when 255 # err packet
        conn.handle_err_packet(packet)
      when 0 # ok packet
        affected_rows = packet.read_lenenc_int
        last_insert_id = packet.read_lenenc_int
        DB::ExecResult.new affected_rows, last_insert_id
      else
        MySql::TextResultSet.new(self, packet.read_lenenc_int(header))
      end
    end
  end
end
