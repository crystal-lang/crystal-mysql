## v0.3.1 (2016-12-24)

* Update to crystal-db ~> 0.3.3
* Fix compatibility issues for crystal 0.20.3
* Add support for Timestamp

## v0.3.0 (2016-12-15)

* Update to crystal-db ~> 0.3.1
* Add support for unprepared statements using TextProtocol. This means only argless commands/query can be executed in unprepared fashion.
* Add support for Bool (stored as BOOL/TINYINT(1))

## v0.2.2 (2016-12-07)

* Remove restriction to use only DB::Any in some cases

## v0.2.1 (2016-12-07)

* Add support for TinyInt as Int8 and SmallInt as Int16. (thanks @crisward)
* Update to crystal 0.20.0 (thanks @tbrand)

## v0.2.0 (2016-10-20)
