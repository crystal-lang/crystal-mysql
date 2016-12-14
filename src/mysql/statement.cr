class MySql::Statement < DB::Statement
  @statement_id : Int32

  def initialize(connection, @sql : String)
    super(connection)
    @statement_id = 0
    params = @params = [] of ColumnSpec
    columns = @columns = [] of ColumnSpec

    conn = self.conn

    # http://dev.mysql.com/doc/internals/en/com-stmt-prepare.html#packet-COM_STMT_PREPARE
    conn.write_packet do |packet|
      packet.write_byte 0x16u8
      packet << @sql
    end

    # http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
    conn.read_packet do |packet|
      conn.raise_if_err_packet packet

      @statement_id = packet.read_int
      num_columns = packet.read_fixed_int(2)
      num_params = packet.read_fixed_int(2)
      packet.read_byte! # reserved_1
      warning_count = packet.read_fixed_int(2)

      conn.read_column_definitions(params, num_params)
      conn.read_column_definitions(columns, num_columns)
    end
  end

  protected def perform_query(args : Enumerable) : MySql::ResultSet
    perform_exec_or_query(args).as(DB::ResultSet)
  end

  protected def perform_exec(args : Enumerable) : DB::ExecResult
    perform_exec_or_query(args).as(DB::ExecResult)
  end

  private def perform_exec_or_query(args : Enumerable)
    conn = self.conn
    conn.write_packet do |packet|
      packet.write_byte 0x17u8
      packet.write_bytes @statement_id.not_nil!, IO::ByteFormat::LittleEndian
      packet.write_byte 0x00u8 # flags: CURSOR_TYPE_NO_CURSOR
      packet.write_bytes 1i32, IO::ByteFormat::LittleEndian

      params = @params.not_nil!
      if params.size > 0
        null_bitmap = BitArray.new(params.size)
        args.each_with_index do |arg, index|
          next unless arg.nil?
          null_bitmap[index] = true
        end
        null_bitmap_slice = null_bitmap.to_slice
        packet.write null_bitmap_slice

        packet.write_byte 0x01u8

        # TODO raise if args.size and params.size does not match
        # params types
        args.each do |arg|
          arg = MySql::Type.to_mysql(arg)
          t = MySql::Type.type_for(arg.class)
          packet.write_byte t.hex_value
          packet.write_byte 0x00u8
        end

        # params values
        args.each do |arg|
          next if arg.nil?
          arg = MySql::Type.to_mysql(arg)
          t = MySql::Type.type_for(arg.class)
          t.write(packet, arg)
        end
      end
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
        MySql::ResultSet.new(self, packet.read_lenenc_int(header))
      end
    end
  end

  protected def conn
    @connection.as(Connection)
  end
end
