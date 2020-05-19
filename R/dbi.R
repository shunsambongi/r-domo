# driver ------------------------------------------------------------------

#' Driver for Domo
#'
#' @keywords internal
#' @export
setClass("DomoDriver", contains = "DBIDriver")

#' @export
#' @rdname DomoDriver-class
setMethod("dbUnloadDriver", "DomoDriver", function(drv, ...) {
  TRUE
})

#' @export
domo <- function() {
  new("DomoDriver")
}


# connection --------------------------------------------------------------

#' Domo connection class
#'
#' @export
#' @keywords internal
#' @name Domo
setClass(
  "DomoConnection",
  contains = "DBIConnection",
  slots = list(
    token = "domo_token"
  )
)

#' @export
setMethod("show", "DomoConnection", function(object) {
  cat_line("<DomoConnection>")
  if (DBI::dbIsValid(object)) {
    vctrs::obj_print_data(object@token)
  } else {
    cat_line("  DISCONNECTED")
  }
})


#' @param drv An object created by [domo()]
#' @rdname Domo
#' @export
setMethod("dbConnect", "DomoDriver", function(
  drv,
  client_id = domo_client_id(),
  client_secret = domo_client_secret(),
  scope = "data"
) {
  token <- domo_token(client_id, client_secret, scope)
  new("DomoConnection", token = token)
})

#' @export
setMethod("dbDisconnect", "DomoConnection", function(conn, ...) {
  conn@token$content <- NULL
  invisible(TRUE)
})

#' @export
setMethod("dbIsValid", "DomoConnection", function(dbObj, ...) {
  !is.null(dbObj@token$content)
})

# result ------------------------------------------------------------------

#' Domo results class
#'
#' @keywords internal
#' @export
setClass(
  "DomoResult",
  contains = "DBIResult",
  slots = list(
    result = "domo_api_result"
  )
)

#' @export
setMethod("dbClearResult", "DomoResult", function(res, ...) {
  TRUE
})

#' Send a query to Domo
#'
#' @export
setMethod("dbSendQuery", c("DomoConnection", "character"), function(
  conn, statement, dataset_id, ...
) {
  result <- query_dataset(conn@token, dataset_id = dataset_id, sql = statement)
  result <- structure(list2env(result), class = class(result))
  new("DomoResult", result = result, ...)
})

#' Retrieve records from Domo query
#' @export
setMethod("dbFetch", "DomoResult", function(res, n = -1, ...) {
  content <- res@result$content
  columns <- content$columns
  rows <- content$rows
  len <- length(rows)

  if (n == 0 || !len) {
    out <- r_data_type(map_chr(content$metadata, function(x) x$type))
    out <- rlang::set_names(out, columns)
    return(dplyr::as_tibble(out))
  }

  if (n < -1) {
    rlang::abort("Invalid `n`")
  }

  if (n == -1) {
    n <- Inf
  }

  n <- min(n, len)

  if (n == len) {
    res@result$content$rows <- list()
  } else {
    res@result$content$rows <- rows[(n + 1):len]
  }

  rows <- map(rows[1:n], function(row) {
    row <- rlang::set_names(row, columns)
    dplyr::as_tibble(row)
  })

  vctrs::vec_rbind(!!!rows)
})

#' @export
setMethod("dbDataType", "DomoConnection", function(dbObj, obj, ...) {
  domo_data_type(obj)
})

#' @export
setMethod("dbHasCompleted", "DomoResult", function(res, ...) {
  !length(res@result$content$rows)
})

#' @export
setMethod(
  "dbCreateTable",
  "DomoConnection",
  function(conn, name, fields, ..., row.names = NULL, temporary = FALSE) {
    result <- create_stream(
      token = conn@token, name = name, schema = fields, ...
    )
    id <- result$content$dataSet$id
    message(glue::glue("Created table {id}"))
    invisible(TRUE)
  }
)

#' @export
setMethod(
  f = "dbWriteTable",
  signature = c("DomoConnection", "character"),
  definition = function(conn, name, value, ...) {
    old <- options("stringsAsFactors" = FALSE)
    on.exit(do.call(options, old), add = TRUE)
    value <- as.data.frame(value)

    result <- search_streams(conn@token, datasource_id = name)
    if (!length(result$content)) {
      rlang::abort(glue::glue("Could not find stream for dataset {name}"))
    } else if (length(result$content) > 1) {
      rlang::abort(glue::glue("Found multiple streams for dataset {name}"))
    }
    stream_id <- result$content[[1L]]$id

    result <- create_stream_execution(conn@token, stream_id)
    exec_id <- result$content$id

    parts <- split_data_parts(value)

    withCallingHandlers({
      iwalk(parts, function(part, part_id) {
        upload_data_part(conn@token, stream_id, exec_id, part_id, part)
      })
      commit_stream_execution(conn@token, stream_id, exec_id)
    }, error = function(e) {
      abort_stream_execution(conn@token, stream_id, exec_id)
    })

    invisible(TRUE)
  }
)

#' @export
setMethod(
  f = "dbReadTable",
  signature = c("DomoConnection", "character"),
  definition = function(conn, name, ...) {
    result <- retrieve_dataset(token = conn@token, dataset_id = name)
    if (result$content$rows == 0) {
      columns <- result$content$schema$columns
      names <- map_chr(columns, function(x) x$name)
      out <- r_data_type(map_chr(columns, function(x) x$type))
      out <- rlang::set_names(out, names)
      return(dplyr::as_tibble(out))
    } else {
      result <- export_dataset(token = conn@token, dataset_id = name, ...)
      result$content
    }
  }
)

#' @export
setMethod("dbListTables", c("DomoConnection"), function(conn, ...) {
  result <- list_datasets(conn@token, ...)
  map_chr(result$content, function(x) x$id)
})

#' @export
setMethod(
  "dbExistsTable",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
    withRestarts(
      withCallingHandlers(
        {
          retrieve_dataset(token = conn@token, dataset_id = name, ...)
          TRUE
        },
        domo_api_error = function(e) {
          if (e$response$status_code == 404) invokeRestart("not_found")
        }
      ),
      not_found = function() FALSE
    )
  }
)

#' @export
setMethod(
  "dbListFields",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
    result <- retrieve_dataset(token = conn@token, dataset_id = name, ...)
    map_chr(result$content$schema$columns, function(x) x$name)
  }
)

#' @export
setMethod(
  "dbRemoveTable",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
    delete_dataset(token = conn@token, dataset_id = name, ...)
    invisible(TRUE)
  }
)
