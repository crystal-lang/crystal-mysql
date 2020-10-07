require "uuid"

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

  def self.type_for(t : ::StaticArray(T, N).class) forall T, N
    MySql::Type::Blob
  end

  def self.type_for(t : ::Time.class)
    MySql::Type::DateTime
  end

  def self.type_for(t : ::Time::Span.class)
    MySql::Type::Time
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
  def self.to_mysql(v : ::UUID)
    v.bytes
  end

  # :nodoc:
  def self.from_mysql(v : Int::Signed) : Bool
    v != 0
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
      MySql::Type::DateTime.parse(str)
    end
  end
  decl_type LongLong, 0x08u8, ::Int64
  decl_type Int24, 0x09u8, ::Int32

  def self.datetime_read(packet)
    MySql::Type::DateTime.read(packet)
  end

  def self.datetime_write(packet, v : ::Time)
    MySql::Type::DateTime.write(packet, v)
  end

  decl_type Date, 0x0au8, ::Time do
    def self.write(packet, v : ::Time)
      self.datetime_write(packet, v)
    end

    def self.read(packet)
      self.datetime_read(packet)
    end

    def self.parse(str : ::String)
      MySql::Type::DateTime.parse(str)
    end
  end
  decl_type Time, 0x0bu8, ::Time::Span do
    def self.write(packet, v : ::Time::Span)
      negative = v.to_i < 0 ? 1 : 0
      d = v.days
      raise ArgumentError.new("MYSQL TIME over 34 days cannot be saved - https://dev.mysql.com/doc/refman/5.7/en/time.html") if d > 34
      microsecond : Int32
      microsecond = (v.nanoseconds // 1000).to_i32
      packet.write_blob UInt8.slice(
        negative, d.to_i8, (d >> 8).to_i8, (d >> 16).to_i8, (d >> 24).to_i8,
        v.hours.to_i8, v.minutes.to_i8, v.seconds.to_i8,
        (microsecond & 0x000000FF).to_u8,
        ((microsecond & 0x0000FF00) >> 8).to_u8,
        ((microsecond & 0x00FF0000) >> 16).to_u8,
        ((microsecond & 0xFF000000) >> 24).to_u8
      )
    end

    def self.read(packet)
      pkt = packet.read_byte!
      return ::Time::Span.new(nanoseconds: 0) if pkt < 1
      negative = packet.read_byte!.to_i32
      days = packet.read_fixed_int(4)
      hour = packet.read_byte!.to_i32
      minute = packet.read_byte!.to_i32
      second = packet.read_byte!.to_i32
      ns = pkt > 8 ? (packet.read_int.to_i32 * 1000) : nil
      time = ns ? ::Time::Span.new(days: days, hours: hour, minutes: minute, seconds: second, nanoseconds: ns) : ::Time::Span.new(days: days, hours: hour, minutes: minute, seconds: second)
      negative > 0 ? (::Time::Span.new(nanoseconds: 0) - time) : time
    end

    def self.parse(str : ::String)
      # TODO replace parsing without using Time parser
      begin
        time = ::Time.parse(str, "%H:%M:%S.%N", location: MySql::TIME_ZONE)
      rescue
        time = ::Time.parse(str, "%H:%M:%S", location: MySql::TIME_ZONE)
      end
      ::Time::Span.new(days: 0, hours: time.hour, minutes: time.minute, seconds: time.second, nanoseconds: time.nanosecond)
    end
  end
  decl_type DateTime, 0x0cu8, ::Time do
    def self.write(packet, v : ::Time)
      v = v.in(location: MySql::TIME_ZONE)
      microsecond : Int32
      microsecond = (v.nanosecond // 1000).to_i32
      packet.write_blob UInt8.slice(
        v.year.to_i16,
        v.year.to_i16 // 256,
        v.month.to_i8, v.day.to_i8,
        v.hour.to_i8, v.minute.to_i8, v.second.to_i8,
        (microsecond & 0x000000FF).to_u8,
        ((microsecond & 0x0000FF00) >> 8).to_u8,
        ((microsecond & 0x00FF0000) >> 16).to_u8,
        ((microsecond & 0xFF000000) >> 24).to_u8
      )
    end

    def self.read(packet)
      pkt = packet.read_byte!
      return ::Time.local(0, 0, 0, location: MySql::TIME_ZONE) if pkt < 1
      year = packet.read_fixed_int(2).to_i32
      month = packet.read_byte!.to_i32
      day = packet.read_byte!.to_i32
      return ::Time.local(year, month, day, location: MySql::TIME_ZONE) if pkt < 6
      hour = packet.read_byte!.to_i32
      minute = packet.read_byte!.to_i32
      second = packet.read_byte!.to_i32
      return ::Time.local(year, month, day, hour, minute, second, location: MySql::TIME_ZONE) if pkt < 8
      ns = packet.read_int.to_i32 * 1000
      return ::Time.local(year, month, day, hour, minute, second, nanosecond: ns, location: MySql::TIME_ZONE)
    end

    def self.parse(str : ::String)
      return ::Time.local(0, 0, 0, location: MySql::TIME_ZONE) if str.starts_with?("0000-00-00")
      begin
        begin
          ::Time.parse(str, "%F %H:%M:%S.%N", location: MySql::TIME_ZONE)
        rescue
          ::Time.parse(str, "%F %H:%M:%S", location: MySql::TIME_ZONE)
        end
      rescue
        ::Time.parse(str, "%F", location: MySql::TIME_ZONE)
      end
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

    def self.write(packet, v : ::StaticArray(T, N)) forall T, N
      packet.write_blob v.to_slice
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
