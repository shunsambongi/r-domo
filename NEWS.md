# domo (development version)

* `dbWriteTable` now updates the dataset schema when `overwrite = TRUE` and the
  schema of the `value` argument does not match the schema of the dataset.
* `dbCreateTable` can now accept a named list of fields for the `fields`
  parameter.
* New `pkgdown` website.
* New `dbGetInfo` methods for `DomoDriver`, `DomoConnection`, and `DomoResult` 
  classes.
* New `dbGetRowsAffected` method for `DomoResult` class. This method always 
  returns 0, since it is not possible to execute SQL.
* New `dbAppendTable` method for `DomoConnection` class.
* Parameterized queries now possible using `dbSendQuery`, `dbBind`, and
  `dbFetch`.
* New `dbColumnInfo`, `dbGetStatement`, and `dbIsValid` methods for 
  `DomoResult`.
* New `DomoTblConnection` class to simplify dbplyr code.
* New `sql_translate_env` method for `DomoTblConnection`.
* New `paste` and `paste0` translations for `DomoTblConnection` class.

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
