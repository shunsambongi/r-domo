# domo (development version)

* Parameterized queries now possible using `dbSendQuery`, `dbBind`, and
  `dbFetch`.
* New `dbColumnInfo`, `dbGetStatement`, and `dbIsValid` methods for 
  `DomoResult`.

# domo 0.2.0

## Breaking changes

* `dbWriteTable` gains `overwrite`, `append` and `stream` parameters. One of 
  `overwrite` or `append` is usually required.
  
## New features

* `dbWriteTable` is now able to append to an existing dataset by setting the
  `append` parameter to `TRUE`.
* `dbWriteTable` is now able to import data into an existing dataset without
  using the stream API by setting the `stream` parameter to `FALSE`.
  
## Minor improvements and fixes

* Added a `NEWS.md` file to track changes to the package.
