class MySql::Driver < DB::Driver
  def connection_builder(uri : URI) : Proc(::DB::Connection)
    params = HTTP::Params.parse(uri.query || "")
    options = connection_options(params)
    mysql_options = MySql::Connection::Options.from_uri(uri)
    ->{ MySql::Connection.new(options, mysql_options).as(::DB::Connection) }
  end
end

DB.register_driver "mysql", MySql::Driver
