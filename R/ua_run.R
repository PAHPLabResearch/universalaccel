# ============================================================
# R/ua_analyze_precomputed.R
# ============================================================

#' Analyze precomputed epoch-level metrics (Part II)
#'
#' Runs the Part II workflow on *precomputed* epoch-level files (e.g., outputs
#' from `accel_summaries()`), producing Outputs 1–5 (bins, zone summaries,
#' percentiles, optional weekly rollups, and a plain-language report).
#'
#' `in_path` may be either:
#'   - a supported file path (csv/txt/xlsx/xls/rds), or
#'   - a folder containing one or more supported files.
#'
#' Folder behavior (important):
#'   - Every file in the folder is processed independently (no combining).
#'   - Each file creates its own output folder (handled inside ua_run_end_to_end()).
#'
#' @param in_path Path to a precomputed file OR a folder containing files.
#' @param out_dir Output directory where UA outputs will be written.
#' @param location Sensor location label used in outputs (e.g., "ndw").
#' @param make_weekly If TRUE, writes weekly rollups (Output4).
#' @param overwrite If TRUE, overwrite existing outputs; otherwise create timestamped variants.
#' @param fail_fast If TRUE, stop on first file error. If FALSE, continue and report failures.
#'
#' @return Invisibly returns a data.table index (one row per file) with status and output folder.
#' @export
ua_analyze_precomputed <- function(in_path,
                                   out_dir,
                                   location = "ndw",
                                   make_weekly = TRUE,
                                   overwrite = TRUE,
                                   fail_fast = FALSE) {

  if (!exists("ua_run_end_to_end", mode = "function")) {
    stop(
      "ua_run_end_to_end() not found in package namespace. ",
      "Make sure R/ua_run_end_to_end.R is included in the package.",
      call. = FALSE
    )
  }
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required.", call. = FALSE)
  }
  library(data.table)

  if (!is.character(in_path) || length(in_path) != 1L || !nzchar(in_path)) {
    stop("in_path must be a single non-empty file or folder path.", call. = FALSE)
  }
  if (!is.character(out_dir) || length(out_dir) != 1L || !nzchar(out_dir)) {
    stop("out_dir must be a single non-empty path.", call. = FALSE)
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  # Normalize without requiring it to exist as a file (folders OK)
  in_path_norm <- tryCatch(normalizePath(in_path, winslash = "/", mustWork = FALSE),
                           error = function(e) in_path)

  # helper
  is_supported <- function(x) grepl("\\.(csv|txt|xlsx|xls|rds)$", x, ignore.case = TRUE)

  # ============================================================
  # Folder path: process ALL files independently
  # ============================================================
  if (dir.exists(in_path_norm)) {

    cand <- list.files(in_path_norm, full.names = TRUE, recursive = FALSE)
    cand <- cand[is_supported(cand)]

    if (!length(cand)) {
      stop(
        "in_path is a folder but contains no supported files (csv/txt/xlsx/xls/rds): ",
        in_path_norm,
        call. = FALSE
      )
    }

    # Sort by filename for deterministic processing (better than mtime)
    cand <- cand[order(tolower(basename(cand)))]

    message("[UA] in_path is a folder; processing ", length(cand), " file(s) independently (no combining).")

    idx <- data.table(
      input_file = cand,
      file_name  = basename(cand),
      status     = "pending",
      error      = NA_character_,
      run_folder = NA_character_
    )

    for (i in seq_along(cand)) {
      f <- cand[i]
      message(sprintf("[UA] (%d/%d) %s", i, length(cand), basename(f)))

      res <- tryCatch(
        {
          # IMPORTANT: ua_run_end_to_end() should create its own per-file folder
          # and (ideally) return run_folder (see note below).
          out <- ua_run_end_to_end(
            in_path     = f,
            out_dir     = out_dir,
            location    = location,
            make_weekly = make_weekly,
            overwrite   = overwrite
          )
          list(ok = TRUE, out = out, err = NULL)
        },
        error = function(e) list(ok = FALSE, out = NULL, err = conditionMessage(e))
      )

      if (isTRUE(res$ok)) {
        idx[i, status := "ok"]

        # If ua_run_end_to_end returns run_folder, capture it.
        # Recommended: have ua_run_end_to_end invisibly return list(run_folder=..., paths=...)
        if (is.list(res$out) && !is.null(res$out$run_folder)) {
          idx[i, run_folder := as.character(res$out$run_folder)]
        }
      } else {
        idx[i, status := "failed"]
        idx[i, error := res$err]
        if (isTRUE(fail_fast)) {
          stop(sprintf("File failed: %s\nReason: %s", basename(f), res$err), call. = FALSE)
        }
      }
    }

    # Write a folder index (professional + easy debugging)
    index_path <- file.path(out_dir, sprintf("UA_FOLDER_INDEX_%s.csv", format(Sys.Date(), "%Y-%m-%d")))
    data.table::fwrite(idx, index_path)
    message("[UA] Wrote folder index: ", index_path)

    return(invisible(idx))
  }

  # ============================================================
  # Single file path: process one file
  # ============================================================
  if (!file.exists(in_path_norm)) {
    stop("in_path does not exist: ", in_path_norm, call. = FALSE)
  }
  if (!is_supported(in_path_norm)) {
    stop("Unsupported file type (allowed: csv/txt, xlsx/xls, rds): ", in_path_norm, call. = FALSE)
  }

  out <- ua_run_end_to_end(
    in_path     = in_path_norm,
    out_dir     = out_dir,
    location    = location,
    make_weekly = make_weekly,
    overwrite   = overwrite
  )

  # Return a tiny index row for consistency
  idx1 <- data.table(
    input_file = in_path_norm,
    file_name  = basename(in_path_norm),
    status     = "ok",
    error      = NA_character_,
    run_folder = if (is.list(out) && !is.null(out$run_folder)) as.character(out$run_folder) else NA_character_
  )
  return(invisible(idx1))
}

#' Run universalaccel precomputed workflow end-to-end
#'
#' `ua_run()` is kept as a stable alias for `ua_analyze_precomputed()`.
#'
#' @inheritParams ua_analyze_precomputed
#' @export
ua_run <- function(in_path,
                   out_dir,
                   location = "ndw",
                   make_weekly = TRUE,
                   overwrite = TRUE,
                   fail_fast = FALSE) {

  ua_analyze_precomputed(
    in_path     = in_path,
    out_dir     = out_dir,
    location    = location,
    make_weekly = make_weekly,
    overwrite   = overwrite,
    fail_fast   = fail_fast
  )
}
