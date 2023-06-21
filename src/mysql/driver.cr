class MySql::Driver < DB::Driver
  class ConnectionBuilder < ::DB::ConnectionBuilder
    def initialize(@options : ::DB::Connection::Options, @mysql_options : MySql::Connection::Options)
    end

    def build : ::DB::Connection
      MySql::Connection.new(@options, @mysql_options)
    end
  end

  def connection_builder(uri : URI) : ::DB::ConnectionBuilder
    params = HTTP::Params.parse(uri.query || "")
    ConnectionBuilder.new(connection_options(params), MySql::Connection::Options.from_uri(uri))
  end
end

DB.register_driver "mysql", MySql::Driver
