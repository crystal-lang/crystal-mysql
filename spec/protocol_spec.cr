require "./spec_helper"

describe MySql::Protocol do
  {% for number in [42, 250, 251, 1042, 65_535, 65_536, 65_542, 16_777_215, 16_777_216, 16_777_242, UInt64::MAX] %}
    it "should write/read LengthEncodedInteger: {{number}}" do
      DB.open "mysql://root@localhost" do |db|
        db.using_connection do |connection|
          content = IO::Memory.new

          # fake header with a 255 packet size
          content.write_bytes(0xFF_u8, IO::ByteFormat::LittleEndian)
          content.write_bytes(0x00_u8, IO::ByteFormat::LittleEndian)
          content.write_bytes(0x00_u8, IO::ByteFormat::LittleEndian)
          content.write_bytes(0x00_u8, IO::ByteFormat::LittleEndian)

          writer = MySql::WritePacket.new(content, connection)
          writer.write_lenenc_int({{number}})
          content.rewind

          reader = MySql::ReadPacket.new(content, connection)
          reader.read_lenenc_int.should eq({{number}})
        end
      end
    end
  {% end %}
end
