require "db"
require "./mysql/*"

module MySql
  record ColumnSpec, catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type_code

  struct ColumnSpec
    def column_type
      MySql::Type.types_by_code[column_type_code]
    end
  end
end
