struct MySql::Result
  def initialize(packet)
    @affected_rows = packet.read_lenenc_int
    @last_insert_id = packet.read_lenenc_int
  end
end
