.onLoad <- function(...) {
  requireNamespace("dbplyr", quietly = TRUE)

  vctrs::s3_register("vctrs::obj_print_header", "domo_token")
  vctrs::s3_register("vctrs::obj_print_data", "domo_token")

  vctrs::s3_register("vctrs::obj_print_header", "domo_result_set")
  vctrs::s3_register("vctrs::obj_print_data", "domo_result_set")
  vctrs::s3_register("vctrs::obj_print_footer", "domo_result_set")

  # dbplyr / lazy tbl
  vctrs::s3_register("dplyr::tbl", "DomoConnection")
  vctrs::s3_register("dplyr::db_query_fields", "DomoConnection")
  vctrs::s3_register("dbplyr::db_collect", "DomoConnection")

  # unsupported ops
  vctrs::s3_register("dplyr::compute", "DomoConnection")
  vctrs::s3_register("dplyr::copy_to", "DomoConnection")
  vctrs::s3_register("dplyr::left_join", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::right_join", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::inner_join", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::full_join", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::anti_join", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::semi_join", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::intersect", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::union", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::union_all", "tbl_DomoConnection")
  vctrs::s3_register("dplyr::setdiff", "tbl_DomoConnection")
}

