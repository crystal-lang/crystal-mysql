class MySql::Statement < DB::Statement
  def initialize(connection, @sql)
    super(connection)

    # http://dev.mysql.com/doc/internals/en/com-stmt-prepare.html#packet-COM_STMT_PREPARE
    @connection.write_packet do |packet|
      packet.write_byte 0x16u8
      packet << @sql
    end

    # http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
    @connection.read_packet do |packet|
      status = packet.read_byte!
      raise "stmt prepare response not ok" unless status == 0

      @statement_id = packet.read_int
      num_columns = packet.read_fixed_int(2)
      num_params = packet.read_fixed_int(2)
      packet.read_byte! # reserved_1
      warning_count = packet.read_fixed_int(2)

      params = @params = [] of ColumnSpec
      @connection.read_column_definitions(params, num_params)

      columns = @columns = [] of ColumnSpec
      @connection.read_column_definitions(columns, num_columns)
    end
  end

  protected def perform_query(args : Slice(DB::Any)) : DB::ResultSet
    perform_exec_or_query(args) as DB::ResultSet
  end

  protected def perform_exec(args : Slice(DB::Any)) : DB::ExecResult
    perform_exec_or_query(args) as DB::ExecResult
  end

  private def perform_exec_or_query(args : Slice(DB::Any))
    @connection.write_packet do |packet|
      packet.write_byte 0x17u8
      packet.write_bytes @statement_id.not_nil!, IO::ByteFormat::LittleEndian
      packet.write_byte 0x00u8 # flags: CURSOR_TYPE_NO_CURSOR
      packet.write_bytes 1i32, IO::ByteFormat::LittleEndian
    end

    @connection.read_packet do |packet|
      case header = packet.read_byte.not_nil!
      when 255 # err packet
        @connection.handle_err_packet(packet)
      when 0 # ok packet
        affected_rows = packet.read_lenenc_int
        last_insert_id = packet.read_lenenc_int
        DB::ExecResult.new affected_rows, last_insert_id
      else
        MySql::ResultSet.new(self, packet.read_lenenc_int(header))
      end
    end
  end
end
