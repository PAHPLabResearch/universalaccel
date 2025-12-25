#' Analyze precomputed epoch-level metrics (Part II)
#'
#' Runs the Part II workflow on *precomputed* epoch-level files (e.g., outputs
#' from `accel_summaries()`), producing Outputs 1–5 (bins, zone summaries,
#' percentiles, optional weekly rollups, and a plain-language report).
#'
#' @param in_path Path to a precomputed CSV (epoch-level metrics).
#' @param out_dir Output directory where UA outputs will be written.
#' @param location Sensor location label used in outputs (e.g., "ndw").
#' @param make_weekly If TRUE, writes weekly rollups (Output4).
#' @param overwrite If TRUE, overwrite existing outputs; otherwise create timestamped variants.
#'
#' @return Invisibly returns whatever `ua_run_end_to_end()` returns.
#' @export
ua_analyze_precomputed <- function(in_path,
                                   out_dir,
                                   location = "ndw",
                                   make_weekly = TRUE,
                                   overwrite = TRUE) {

  if (!exists("ua_run_end_to_end", mode = "function")) {
    stop(
      "ua_run_end_to_end() not found in package namespace. ",
      "Make sure R/ua_run_end_to_end.R is included in the package.",
      call. = FALSE
    )
  }

  ua_run_end_to_end(
    in_path     = in_path,
    out_dir     = out_dir,
    location    = location,
    make_weekly = make_weekly,
    overwrite   = overwrite
  )
}

#' Run universalaccel precomputed workflow end-to-end (legacy name)
#'
#' `ua_run()` is kept as a stable alias for `ua_analyze_precomputed()`.
#'
#' @inheritParams ua_analyze_precomputed
#' @export
ua_run <- function(in_path,
                   out_dir,
                   location = "ndw",
                   make_weekly = TRUE,
                   overwrite = TRUE) {

  ua_analyze_precomputed(
    in_path     = in_path,
    out_dir     = out_dir,
    location    = location,
    make_weekly = make_weekly,
    overwrite   = overwrite
  )
}
