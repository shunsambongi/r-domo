# zzz.R
tbl.DomoConnection <- function(src, from, ...) {
  attr(src, "domo_dataset_id") <- from
  from <- dbplyr::as.sql("table")
  NextMethod()
}

# zzz.R
db_query_fields.DomoConnection <- function(con, sql, ...) {
  sql <- dplyr::sql_select(
    con = con,
    select = dbplyr::sql("*"),
    from = dplyr::sql_subquery(con, sql),
    where = dbplyr::sql("0 = 1")
  )
  qry <- DBI::dbSendQuery(con, sql, attr(con, "domo_dataset_id"))
  on.exit(DBI::dbClearResult(qry))
  res <- DBI::dbFetch(qry, 0)
  names(res)
}

# zzz.R
db_collect.DomoConnection <- function(con, sql, n = -1, warn_incomplete = TRUE, ...) {
  res <- DBI::dbSendQuery(con, sql, attr(con, "domo_dataset_id"))
  tryCatch({
    out <- DBI::dbFetch(res, n = n)
    if (warn_incomplete && !DBI::dbHasCompleted(res)) {
      warning(
        "Not all rows retrieved. Use n = -1 to retrieve all", call. = FALSE
      )
    }
  }, finally = {
    DBI::dbClearResult(res)
  })
  out
}


# unsupported ops ---------------------------------------------------------

stop_unsupported <- function(name) {
  rlang::abort(
    paste0("The function `", name, "` is unsupported for Domo."),
    class = "domo_unsupported_method"
  )
}

# zzz.R
copy_to.DomoConnection <- function(
  dest, ...
) {
  stop_unsupported("copy_to")
}

# zzz.R
compute.DomoConnection <- function(x, ...) {
  stop_unsupported("compute")
}

# zzz.R
left_join.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("left_join")
}

# zzz.R
right_join.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("right_join")
}

# zzz.R
inner_join.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("inner_join")
}

# zzz.R
full_join.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("full_join")
}

# zzz.R
anti_join.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("anti_join")
}

# zzz.R
semi_join.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("semi_join")
}

# zzz.R
intersect.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("intersect")
}

# zzz.R
union.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("union")
}

# zzz.R
union_all.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("union_all")
}

# zzz.R
setdiff.tbl_DomoConnection <- function(x, y, ...) {
  stop_unsupported("setdiff")
}



# formatting --------------------------------------------------------------

#' @export
format.tbl_DomoConnection <- function(x, ...) {
  source <- format_tbl_source(x)
  domain <- format_tbl_domain(x)

  out <- dplyr::trunc_mat(x, ...)
  out$summary <- format_trunc_mat_summary(out, source, domain)
  format(out)
}

format_tbl_source <- function(x) {
  dataset_id <- attr(x$src$con, "domo_dataset_id") %||% "unknown"
  sprintf("dataset<%s> [?? x %d]", dataset_id, ncol(x))
}

format_tbl_domain <- function(x) {
  x$src$con@token$content$domain
}

format_trunc_mat_summary <- function(x, source, domain) {
  summary <- x$summary

  # fix names
  nms <- names(summary)
  nms[nms == "Database"] <- "Domain"
  names(summary) <- nms

  # set values
  summary[["Source"]] <- source
  summary[["Domain"]] <- domain

  # cast / fix names
  summary <- as.vector(summary, "character")
  names(summary) <- nms
  summary
}
