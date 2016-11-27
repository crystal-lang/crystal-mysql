module DB
  # Methods to allow querying a database.
  # All methods accepts a `query : String` and a set arguments.
  #
  # Three kind of statements can be performed:
  #  1. `#exec` waits no record response from the database. An `ExecResult` is returned.
  #  2. `#scalar` reads a single value of the response. A union of possible values is returned.
  #  3. `#query` returns a `ResultSet` that allows iteration over the rows in the response and column information.
  #
  # Arguments can be passed by position
  #
  # ```
  # db.query("SELECT name FROM ... WHERE age > ?", age)
  # ```
  #
  # Convention of mapping how arguments are mapped to the query depends on each driver.
  #
  # Including `QueryMethods` requires a `prepare(query) : Statement` method that is not expected
  # to be called directly.
  module QueryMethods
    # :nodoc:
    abstract def prepare(query) : Statement

    # Executes a *query* and returns a `ResultSet` with the results.
    # The `ResultSet` must be closed manually.
    #
    # ```
    # result = db.query "select name from contacts where id = ?", 10
    # begin
    #   if result.move_next
    #     id = result.read(Int32)
    #   end
    # ensure
    #   result.close
    # end
    # ```
    def query(query, *args)
      prepare(query).query(*args)
    end

    # Executes a *query* and yields a `ResultSet` with the results.
    # The `ResultSet` is closed automatically.
    #
    # ```
    # db.query("select name from contacts where age > ?", 18) do |rs|
    #   rs.each do
    #     name = rs.read(String)
    #   end
    # end
    # ```
    def query(query, *args)
      # CHECK prepare(query).query(*args, &block)
      rs = query(query, *args)
      yield rs ensure rs.close
    end

    # Executes a *query* that expects a single row and yields a `ResultSet`
    # positioned at that first row.
    #
    # The given block must not invoke `move_next` on the yielded result set.
    #
    # Raises `DB::Error` if there were no rows, or if there were more than one row.
    #
    # ```
    # name = db.query_one "select name from contacts where id = ?", 18, &.read(String)
    # ```
    def query_one(query, *args, &block : ResultSet -> U) : U
      query(query, *args) do |rs|
        raise DB::Error.new("no rows") unless rs.move_next

        value = yield rs
        raise DB::Error.new("more than one row") if rs.move_next
        return value
      end
    end

    # Executes a *query* that expects a single row and returns it
    # as a tuple of the given *types*.
    #
    # Raises `DB::Error` if there were no rows, or if there were more than one row.
    #
    # ```
    # db.query_one "select name, age from contacts where id = ?", 1, as: {String, Int32}
    # ```
    def query_one(query, *args, as types : Tuple)
      query_one(query, *args) do |rs|
        rs.read(*types)
      end
    end

    # Executes a *query* that expects a single row
    # and returns the first column's value as the given *type*.
    #
    # Raises `DB::Error` if there were no rows, or if there were more than one row.
    #
    # ```
    # db.query_one "select name from contacts where id = ?", 1, as: String
    # ```
    def query_one(query, *args, as type : Class)
      query_one(query, *args) do |rs|
        rs.read(type)
      end
    end

    # Executes a *query* that expects at most a single row and yields a `ResultSet`
    # positioned at that first row.
    #
    # Returns `nil`, not invoking the block, if there were no rows.
    #
    # Raises `DB::Error` if there were more than one row
    # (this ends up invoking the block once).
    #
    # ```
    # name = db.query_one? "select name from contacts where id = ?", 18, &.read(String)
    # typeof(name) # => String | Nil
    # ```
    def query_one?(query, *args, &block : ResultSet -> U) : U?
      query(query, *args) do |rs|
        return nil unless rs.move_next

        value = yield rs
        raise DB::Error.new("more than one row") if rs.move_next
        return value
      end
    end

    # Executes a *query* that expects a single row and returns it
    # as a tuple of the given *types*.
    #
    # Returns `nil` if there were no rows.
    #
    # Raises `DB::Error` if there were more than one row.
    #
    # ```
    # result = db.query_one? "select name, age from contacts where id = ?", 1, as: {String, Int32}
    # typeof(result) # => Tuple(String, Int32) | Nil
    # ```
    def query_one?(query, *args, as types : Tuple)
      query_one?(query, *args) do |rs|
        rs.read(*types)
      end
    end

    # Executes a *query* that expects a single row
    # and returns the first column's value as the given *type*.
    #
    # Returns `nil` if there were no rows.
    #
    # Raises `DB::Error` if there were more than one row.
    #
    # ```
    # name = db.query_one? "select name from contacts where id = ?", 1, as: String
    # typeof(name) # => String?
    # ```
    def query_one?(query, *args, as type : Class)
      query_one?(query, *args) do |rs|
        rs.read(type)
      end
    end

    # Executes a *query* and yield a `ResultSet` positioned at the beginning
    # of each row, returning an array of the values of the blocks.
    #
    # ```
    # names = db.query_all "select name from contacts", &.read(String)
    # ```
    def query_all(query, *args, &block : ResultSet -> U) : Array(U)
      ary = [] of U
      query(query, *args) do |rs|
        rs.each do
          ary.push(yield rs)
        end
      end
      ary
    end

    # Executes a *query* and returns an array where each row is
    # read as a tuple of the given *types*.
    #
    # ```
    # contacts = db.query_all "select name, age from contactas", as: {String, Int32}
    # ```
    def query_all(query, *args, as types : Tuple)
      query_all(query, *args) do |rs|
        rs.read(*types)
      end
    end

    # Executes a *query* and returns an array where there first
    # column's value of each row is read as the given *type*.
    #
    # ```
    # names = db.query_all "select name from contactas", as: String
    # ```
    def query_all(query, *args, as type : Class)
      query_all(query, *args) do |rs|
        rs.read(type)
      end
    end

    # Performs the `query` and returns an `ExecResult`
    def exec(query, *args)
      prepare(query).exec(*args)
    end

    # Performs the `query` and returns a single scalar value
    # puts db.scalar("SELECT MAX(name)").as(String) # => (a String)
    def scalar(query, *args)
      prepare(query).scalar(*args)
    end
  end
end
