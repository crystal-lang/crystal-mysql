## v0.14.0 (2021-01-27)

* Update to crystal-db ~> 0.11.0. ([#101](https://github.com/crystal-lang/crystal-mysql/pull/101))
* Migrate CI to GitHub Actions. ([#102](https://github.com/crystal-lang/crystal-mysql/pull/102), thanks @bcardiff)

This release requires Crystal 1.0.0 or later.

## v0.13.0 (2021-01-26)

* Add `UUID` support. ([#76](https://github.com/crystal-lang/crystal-mysql/pull/76), thanks @kalinon)

## v0.12.0 (2020-09-30)

* Update to crystal-db ~> 0.10.0. ([#95](https://github.com/crystal-lang/crystal-mysql/pull/95))

This release requires Crystal 0.35.0 or later.

## v0.11.2 (2020-06-19)

* Fix compatibility issues for Crystal 0.35.1. ([#92](https://github.com/crystal-lang/crystal-mysql/pull/92), thanks @bcardiff)

## v0.11.1 (2020-06-09)

* Fix compatibility issues for Crystal 0.35.0. ([#90](https://github.com/crystal-lang/crystal-mysql/pull/90), thanks @bcardiff)

## v0.11.0 (2020-04-06)

* Update to crystal-db ~> 0.9.0. ([#88](https://github.com/crystal-lang/crystal-mysql/pull/88), thanks @bcardiff)
* Fix readme sample. ([#87](https://github.com/crystal-lang/crystal-mysql/pull/87), thanks @drum445)

## v0.10.0 (2019-12-11)

* Update to crystal-db ~> 0.8.0. ([#85](https://github.com/crystal-lang/crystal-mysql/pull/85))

## v0.9.0 (2019-09-20)

This release requires Crystal >= 0.30.0

* Update to crystal-db ~> 0.7.0

## v0.8.0 (2019-08-02)

* Fix compatibility issues for Crystal 0.30.0. ([#80](https://github.com/crystal-lang/crystal-mysql/pull/80), thanks @bcardiff)

## v0.7.0 (2019-07-03)

* Support implicit conversion from integer types to Bool ([#78](https://github.com/crystal-lang/crystal-mysql/pull/78), thanks @Blacksmoke16)
* Improve docs. ([#71](https://github.com/crystal-lang/crystal-mysql/pull/71), thanks @fernandes)

## v0.6.0 (2019-04-18)

* Fix compatibility issues for crystal 0.28.0 ([#73](https://github.com/crystal-lang/crystal-mysql/pull/73))
* Fix connection to IPv6 hosts ([#69](https://github.com/crystal-lang/crystal-mysql/pull/69), thanks @j8r)

## v0.5.1 (2018-11-06)

* Fix `read_lenenc_int` return `UInt64`.
* Add missing `IO#read_fully` when reading slice. ([#45](https://github.com/crystal-lang/crystal-mysql/pull/45), thanks @pacuum).

## v0.5.0 (2018-06-15)

* Fix compatibility issues for crystal 0.25.0 ([#60](https://github.com/crystal-lang/crystal-mysql/pull/60))
  * All the time instances are translated to UTC before saving them in the db
* Send quit packet before closing connection ([#61](https://github.com/crystal-lang/crystal-mysql/pull/61), thanks @liuyang1204)

## v0.4.0 (2017-12-29)

* Update to crystal-db ~> 0.5.0
* Fix compatibility issues for crystal 0.24.1 (thanks @lipanski)
  * Drop support for zero dates

## v0.3.3 (2017-11-08)

* Fix release connection. (see [#35](https://github.com/crystal-lang/crystal-mysql/pull/35) and [#38](https://github.com/crystal-lang/crystal-mysql/pull/38), thanks @benoist)
* Fix unprepared queries creation. ([#37](https://github.com/crystal-lang/crystal-mysql/pull/37), thanks @benoist)
* Fix use read_fully when reading slice. (see [#25](https://github.com/crystal-lang/crystal-mysql/issues/25))
* Add support for Date, Time and Mediumint. (see [#31](https://github.com/crystal-lang/crystal-mysql/pull/31) and [#41](https://github.com/crystal-lang/crystal-mysql/pull/41), thanks @crisward)

## v0.3.2 (2017-03-21)

* Update to crystal-db ~> 0.4.0

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
