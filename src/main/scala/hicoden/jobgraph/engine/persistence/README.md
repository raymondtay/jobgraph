Notes:
-----------

# About using Doobie 0.5.x

Warning: this might not be accurate for subsequent releases of Doobie (beyond
version 0.5.x)

## Postgresql Custom Types

Doobie's `Fragments` is where this shines as it does not support out of the
box; the programming isn't that difficult.

## Unit tests

For now, Doobie doesn't present a embedded Postgresql server for running the
test database against; hence, the unit-tests cannot actually be tested on a
running database server. This has been reflected to the developers of Doobie
and an implementation is likely to happen at a later release.

