class MySql::Driver < DB::Driver
  def build_connection(db : DB::Database)
    MySql::Connection.new(db)
  end
end

DB.register_driver "mysql", MySql::Driver
