#' Analyze precomputed epoch-level metrics (Part II)
#'
#' Runs the Part II workflow on *precomputed* epoch-level files (e.g., outputs
#' from `accel_summaries()`), producing Outputs 1–5 (bins, zone summaries,
#' percentiles, optional weekly rollups, and a plain-language report).
#'
#' `in_path` may be either:
#'   - a supported file path (csv/txt/xlsx/xls/rds), or
#'   - a folder containing at least one supported file (newest file is used).
#'
#' @param in_path Path to a precomputed file OR a folder containing one.
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

  # ---------------------------
  # Folder-friendly in_path
  # ---------------------------
  if (!is.character(in_path) || length(in_path) != 1L || !nzchar(in_path)) {
    stop("in_path must be a single non-empty file or folder path.", call. = FALSE)
  }

  # Normalize without requiring it to exist as a file (folders OK)
  in_path <- tryCatch(normalizePath(in_path, winslash = "/", mustWork = FALSE),
                      error = function(e) in_path)

  # If user passed a directory, pick newest supported file inside
  if (dir.exists(in_path)) {
    cand <- list.files(in_path, full.names = TRUE, recursive = FALSE)

    keep <- grepl("\\.(csv|txt|xlsx|xls|rds)$", cand, ignore.case = TRUE)
    cand <- cand[keep]

    if (!length(cand)) {
      stop(
        "in_path is a folder but contains no supported files (csv/txt/xlsx/xls/rds): ",
        in_path,
        call. = FALSE
      )
    }

    ord <- order(file.info(cand)$mtime, decreasing = TRUE)
    in_path_file <- cand[ord[1]]

    message("[UA] in_path was a folder; using newest file: ", basename(in_path_file))
    in_path <- in_path_file
  }

  # If it's not a directory, it must be an existing file
  if (!file.exists(in_path)) {
    stop("in_path does not exist: ", in_path, call. = FALSE)
  }

  # Continue as usual
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
