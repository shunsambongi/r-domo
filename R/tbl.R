# DomoTblConnection -------------------------------------------------------

#' DomoTblConnection class
#'
#' @export
#' @keywords internal
setClass(
  "DomoTblConnection",
  contains = "DomoConnection",
  slots = list(dataset_id = "character")
)

domo_tbl_send_query <- function(conn, statement, ...) {
  callNextMethod(conn, statement, conn@dataset_id, ...)
}

#' @export
#' @rdname DomoTblConnection-class
setMethod(
  "dbSendQuery",
  c("DomoTblConnection", "character"),
  domo_tbl_send_query
)


# tbl ---------------------------------------------------------------------

# zzz.R
tbl.DomoConnection <- function(src, from, ...) {
  src <- as(src, "DomoTblConnection")
  src@dataset_id <- from
  from <- dbplyr::as.sql("table")
  NextMethod()
}


# sql ---------------------------------------------------------------------

#' Simulate Domo Connection
#'
#' For testing SQL translations
#'
#' @export
simulate_domo <- function() {
  dbplyr::simulate_dbi(c("DomoTblConnection", "DomoConnection"))
}

# zzz.R
sql_translate_env.DomoTblConnection <- function(con) {
  dbplyr::sql_variant(
    scalar = dbplyr::sql_translator(
      paste = sql_paste(" "),
      paste0 = sql_paste(""),
      .parent = dbplyr::base_scalar
    ),
    aggregate = dbplyr::base_agg,
    window = dbplyr::base_win
  )
}

sql_paste <- function(default_sep) {
  force(default_sep)
  function(..., sep = default_sep, collapse = NULL) {
    if (!is.null(collapse)) {
      stop(
        "`collapse` not supported in Domo translation of `paste()`.\n",
        call. = FALSE
      )
    }
    args <- rlang::quos(...)
    if (sep != "") {
      args <- c(rbind(sep, args))[-1]
    }
    expr <- rlang::expr(dbplyr::sql_call2("CONCAT", !!!args))
    rlang::eval_tidy(expr)
  }
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
format.tbl_DomoTblConnection <- function(x, ...) {
  source <- format_tbl_source(x)
  domain <- format_tbl_domain(x)

  out <- dplyr::trunc_mat(x, ...)
  out$summary <- format_trunc_mat_summary(out, source, domain)
  format(out)
}

format_tbl_source <- function(x) {
  dataset_id <- x$src$con@dataset_id %||% "unknown"
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
