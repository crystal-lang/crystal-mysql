# crystal-mysql [![Build Status](https://travis-ci.org/crystal-lang/crystal-mysql.svg?branch=master)](https://travis-ci.org/crystal-lang/crystal-mysql)


MySQL driver implement natively in Crystal, without relying on external libraries.

Check [crystal-db](https://github.com/crystal-lang/crystal-db) for general db driver documentation. crystal-mysql driver is registered under `mysql://` uri.

## Why

Using a natively implemented library has a significant performance improvement over working with an external library, since there is no need to copy data to and from the Crystal space and the native code. Initial tests with the library have shown a 2x-3x performance boost, though additional testing is required.

Also, going through the MySQL external library *blocks* the Crystal thread using it, thus imposing a significant penalty to concurrent database accesses, such as those in web servers. We aim to overcome this issue through a full Crystal implementation of the MySQL driver that plays nice with non-blocking IO.

## Status

This driver is a work in progress.
It implements mysql's binary protocol to create prepared statements.
Contributions are most welcome.

## Installation

Add this to your application's `shard.yml`:

```yml
dependencies:
  mysql:
    github: crystal-lang/crystal-mysql
```

## Usage

```crystal
require "mysql"

# connect to localhost mysql test db
DB.open "mysql://root@localhost/test" do |db|
  db.exec "drop table if exists contacts"
  db.exec "create table contacts (name varchar(30), age int)"
  db.exec "insert into contacts values (?, ?)", "John Doe", 30

  args = [] of DB::Any
  args << "Sarah"
  args << 33
  db.exec "insert into contacts values (?, ?)", args: args

  puts "max age:"
  puts db.scalar "select max(age) from contacts" # => 33

  puts "contacts:"
  db.query "select name, age from contacts order by age desc" do |rs|
    puts "#{rs.column_name(0)} (#{rs.column_name(1)})"
    # => name (age)
    rs.each do
      puts "#{rs.read(String)} (#{rs.read(Int32)})"
      # => Sarah (33)
      # => John Doe (30)
    end
  end
end
```

When running this example, if you get the following exception:

> Unhandled exception: Client does not support authentication protocol requested by server; consider upgrading MySQL client (Exception)

You have two options, set a password for root, or (most recommended option) create another user with access to `test` database.

```mysql
CREATE USER 'test'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';
GRANT ALL PRIVILEGES ON test.* TO 'test'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
quit
```

Then use the example above changing the `DB.open` line to

```crystal
DB.open "mysql://test:yourpassword@localhost/test" do |db|
```

### Connection URI

The connection string has the following syntax:

```
mysql://[user[:[password]]@]host[:port][/schema][?param1=value1&param2=value2]
```

#### Transport

The driver supports tcp connection or unix sockets

- `mysql://localhost` will connect using tcp and the default MySQL port 3306.
- `mysql://localhost:8088` will connect using tcp using port 8088.
- `mysql:///path/to/other.sock` will connect using unix socket `/path/to/other.sock`.

Any of the above can be used with `user@` or `user:password@` to pass credentials.

#### Default database

A `database` query string will specify the default database. 
Connection strings with a host can also use the first path component to specify the default database.
Query string takes precedence because it's more explicit.

- `mysql://localhost/mydb`
- `mysql://localhost:3306/mydb`
- `mysql://localhost:3306?database=mydb`
- `mysql:///path/to/other.sock?database=mydb`

#### Secure connections (SSL/TLS)

By default a tcp connection will establish a secure connection, whether a unix socket will not.

You can tweak this default behaviour and require further validation of certificates using `ssl-mode` and the following query strings.

- `ssl-mode`: Either `disabled`, `preferred` (default), `required`, `verify_ca`, `verify_identity`.
- `ssl-key`: Path to the client key.
- `ssl-cert`: Path to the client certificate.
- `ssl-ca`: Path to the CA certificate.

#### Other query params

- `encoding`: The collation & charset (character set) to use during the connection.
            If empty or not defined, it will be set to `utf8_general_ci`.
            The list of available collations is defined in [`MySql::Collations::COLLATIONS_IDS_BY_NAME`](src/mysql/collations.cr)
