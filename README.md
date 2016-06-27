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
  db.exec "insert into contacts values (?, ?)", args

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