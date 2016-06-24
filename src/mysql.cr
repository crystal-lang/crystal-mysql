require "db"
require "./mysql/*"

module MySql
  record ColumnSpec, catalog : String, schema : String, table : String, org_table : String, name : String, org_name : String, character_set : Int64, column_length : Int16, column_type_code : UInt8

  struct ColumnSpec
    def column_type
      MySql::Type.types_by_code[column_type_code]
    end
  end
end
