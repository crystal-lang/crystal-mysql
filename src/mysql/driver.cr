class MySql::Driver < DB::Driver
  def build_connection(context : DB::ConnectionContext) : MySql::Connection
    MySql::Connection.new(context)
  end
end

DB.register_driver "mysql", MySql::Driver
