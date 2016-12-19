# :nodoc:
abstract struct MySql::Type
  # Column types
  # http://dev.mysql.com/doc/internals/en/com-query-response.html#column-type

  @@types_by_code = Hash(UInt8, MySql::Type.class).new
  @@hex_value : UInt8 = 0x00u8

  def self.hex_value
    @@hex_value
  end

  def self.types_by_code
    @@types_by_code
  end

  # Returns which MySql::Type should be used to encode values of type *t*.
  # Used when sending query params.
  def self.type_for(t : ::Int8.class)
    MySql::Type::Tiny
  end

  def self.type_for(t : ::Int16.class)
    MySql::Type::Short
  end

  def self.type_for(t : ::Int32.class)
    MySql::Type::Long
  end

  def self.type_for(t : ::Int64.class)
    MySql::Type::LongLong
  end

  def self.type_for(t : ::Float32.class)
    MySql::Type::Float
  end

  def self.type_for(t : ::Float64.class)
    MySql::Type::Double
  end

  def self.type_for(t : ::String.class)
    MySql::Type::String
  end

  def self.type_for(t : ::Bytes.class)
    MySql::Type::Blob
  end

  def self.type_for(t : ::Time.class)
    MySql::Type::DateTime
  end

  def self.type_for(t : ::Nil.class)
    MySql::Type::Null
  end

  def self.type_for(t)
    raise "MySql::Type does not support #{t} values"
  end

  def self.db_any_type
    raise "not implemented"
  end

  # Writes in packet the value in ProtocolBinary format.
  # Used when sending query params.
  def self.write(packet, v)
    raise "not supported write"
  end

  # Reads from packet a value in ProtocolBinary format of the type
  # specified by self.
  def self.read(packet)
    raise "not supported read"
  end

  # Parse from str a value in TextProtocol format of the type
  # specified by self.
  def self.parse(str : ::String)
    raise "not supported"
  end

  # :nodoc:
  def self.to_mysql(v)
    v
  end

  # :nodoc:
  def self.to_mysql(v : Bool)
    v ? 1i8 : 0i8
  end

  # :nodoc:
  def self.from_mysql(v : Int8)
    v != 0i8
  end

  macro decl_type(name, value, db_any_type = nil)
    struct {{name}} < Type
      @@hex_value = {{value}}

      {% if db_any_type %}
      def self.db_any_type
        {{db_any_type}}
      end

      def self.write(packet, v : {{db_any_type}})
        packet.write_bytes v, IO::ByteFormat::LittleEndian
      end

      def self.read(packet)
        packet.read_bytes {{db_any_type}}, IO::ByteFormat::LittleEndian
      end

      def self.parse(str : ::String)
        {{db_any_type}}.new(str)
      end
      {% end %}

      {{yield}}
    end

    Type.types_by_code[{{value}}] = {{name}}
  end

  decl_type Decimal, 0x00u8
  decl_type Tiny, 0x01u8, ::Int8
  decl_type Short, 0x02u8, ::Int16
  decl_type Long, 0x03u8, ::Int32
  decl_type Float, 0x04u8, ::Float32
  decl_type Double, 0x05u8, ::Float64
  decl_type Null, 0x06u8, ::Nil do
    def self.read(packet)
      nil
    end

    def self.parse(str : ::String)
      nil
    end
  end
  decl_type Timestamp, 0x07u8, ::Time do
    def self.write(packet, v : ::Time)
      MySql::Type::DateTime.write(packet, v)
    end

    def self.read(packet)
      MySql::Type::DateTime.read(packet)
    end

    def self.parse(str : ::String)
      raise "TextProtocol::Timestamp not implemented"
    end
  end
  decl_type LongLong, 0x08u8, ::Int64
  decl_type Int24, 0x09u8
  decl_type Date, 0x0au8
  decl_type Time, 0x0bu8
  decl_type DateTime, 0x0cu8, ::Time do
    def self.write(packet, v : ::Time)
      packet.write_blob UInt8.slice(v.year.to_i16, v.year.to_i16/256, v.month.to_i8, v.day.to_i8, v.hour.to_i8, v.minute.to_i8, v.second.to_i8, v.millisecond*1000, v.millisecond*1000/256, v.millisecond*1000/65536)
    end

    def self.read(packet)
      pkt = packet.read_byte!
      return ::Time.new(0) if pkt < 1
      year = packet.read_fixed_int(2).to_i32
      month = packet.read_byte!.to_i32
      day = packet.read_byte!.to_i32
      return ::Time.new(year, month, day) if pkt < 6
      hour = packet.read_byte!.to_i32
      minute = packet.read_byte!.to_i32
      second = packet.read_byte!.to_i32
      return ::Time.new(year, month, day, hour, minute, second) if pkt < 8
      ms = packet.read_int.to_i32 / 1000 # returns microseconds, time only supports milliseconds
      return ::Time.new(year, month, day, hour, minute, second, ms)
    end

    def self.parse(str : ::String)
      raise "TextProtocol::Time not implemented"
    end
  end
  decl_type Year, 0x0du8
  decl_type VarChar, 0x0fu8
  decl_type Bit, 0x10u8
  decl_type NewDecimal, 0xf6u8, ::Float64 do
    def self.read(packet)
      packet.read_lenenc_string.to_f64
    end
  end
  decl_type Enum, 0xf7u8
  decl_type Set, 0xf8u8
  decl_type TinyBlob, 0xf9u8
  decl_type MediumBlob, 0xfau8
  decl_type LongBlob, 0xfbu8
  decl_type Blob, 0xfcu8, ::Bytes do
    def self.write(packet, v : ::Bytes)
      packet.write_blob v
    end

    def self.read(packet)
      packet.read_blob
    end

    def self.parse(str : ::String)
      str.to_slice
    end
  end
  decl_type VarString, 0xfdu8, ::String do
    def self.write(packet, v : ::String)
      packet.write_lenenc_string v
    end

    def self.read(packet)
      packet.read_lenenc_string
    end

    def self.parse(str : ::String)
      str
    end
  end
  decl_type String, 0xfeu8, ::String do
    def self.write(packet, v : ::String)
      packet.write_lenenc_string v
    end

    def self.read(packet)
      packet.read_lenenc_string
    end

    def self.parse(str : ::String)
      str
    end
  end
  decl_type Geometry, 0xffu8
end
