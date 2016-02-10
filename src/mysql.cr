require "db"
require "./mysql/*"

module MySql
  record ColumnSpec, catalog, schema, table, org_table, name, org_name, character_set, column_length, column_type
end
