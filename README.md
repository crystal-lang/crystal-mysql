# crystal-mysql

MySQL driver implemented natively in Crystal, without relying on external libraries.

## Why

Using a natively implemented library has a significant performance improvement over working with an external library, since there is no need to copy data to and from the Crystal space and the native code. Initial tests with the library have shown a 2x-3x performance boost, though additional testing is required.

Also, going through the MySQL external library *blocks* the Crystal thread using it, thus imposing a significant penalty to concurrent database accesses, such as those in web servers. We aim to overcome this issue through a full Crystal implementation of the MySQL driver that plays nice with non-blocking IO.

## Status

This driver is a work in progress.
Contributions are most welcome.
