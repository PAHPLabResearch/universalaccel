# ============================================================
# R/ua_analyze_precomputed.R
# ============================================================

#' Analyze precomputed epoch-level metrics (Part II)
#'
#' Runs the Part II workflow on precomputed epoch-level files.
#'
#' `in_path` may be either:
#'   - a supported file path (csv/txt/xlsx/xls/rds), or
#'   - a folder containing one or more supported files.
#'
#' Folder behavior:
#'   - Every file in the folder is processed independently (no combining).
#'   - Each file creates its own output folder under `out_dir/UA_runs/`.
#'
#' @param in_path Path to a precomputed file OR a folder containing files.
#' @param out_dir Output directory where UA outputs will be written.
#' @param location Sensor location label used in outputs (e.g., "ndw").
#' @param make_weekly If TRUE, writes weekly rollups where applicable.
#' @param overwrite If TRUE, overwrite existing outputs; otherwise create timestamped variants.
#' @param fail_fast If TRUE, stop on first file error. If FALSE, continue and report failures.
#' @param outputs Character vector of requested outputs. Allowed:
#'   "output1","output2","output3","output4","output5","output6".
#'
#'   Current meaning:
#'   - output1 = daily binned intensity distribution
#'   - output2 = IG & Volume (daily + weekly)
#'   - output3 = daily MX
#'   - output4 = weekly MX
#'   - output5 = daily intensity bin summary + NHANES references
#'   - output6 = IG & Volume percentiles in NHANES
#'
#'   NHANES-referenced outputs currently require epoch 5 or 60.
#'
#' @return Invisibly returns a data.table index (one row per file) with status and output folder.
#' @export
ua_analyze_precomputed <- function(in_path,
                                   out_dir,
                                   location = "ndw",
                                   make_weekly = TRUE,
                                   overwrite = TRUE,
                                   fail_fast = FALSE,
                                   outputs = c("output1", "output2", "output3", "output4", "output5", "output6")) {

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

  outputs <- unique(tolower(as.character(outputs)))
  valid_outputs <- paste0("output", 1:6)
  bad_outputs <- setdiff(outputs, valid_outputs)
  if (length(bad_outputs)) {
    stop("Unknown outputs requested: ", paste(bad_outputs, collapse = ", "), call. = FALSE)
  }

  if (!is.character(in_path) || length(in_path) != 1L || !nzchar(in_path)) {
    stop("in_path must be a single non-empty file or folder path.", call. = FALSE)
  }
  if (!is.character(out_dir) || length(out_dir) != 1L || !nzchar(out_dir)) {
    stop("out_dir must be a single non-empty path.", call. = FALSE)
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  in_path_norm <- tryCatch(
    normalizePath(in_path, winslash = "/", mustWork = FALSE),
    error = function(e) in_path
  )

  is_supported <- function(x) {
    ok_ext <- grepl("\\.(csv|txt|xlsx|xls|rds)$", x, ignore.case = TRUE)
    bad_name <- grepl(
      "^(UA_processing_log_|UA_processing_summary_|UA_FOLDER_INDEX_|UA_Output[0-9]+_)",
      basename(x),
      ignore.case = TRUE
    )
    ok_ext & !bad_name
  }

  extract_run_folder <- function(run_obj) {
    rf <- NA_character_

    if (is.list(run_obj) &&
        !is.null(run_obj$run_folder) &&
        length(run_obj$run_folder) == 1L &&
        !is.na(run_obj$run_folder) &&
        nzchar(run_obj$run_folder)) {
      rf <- as.character(run_obj$run_folder)
      return(rf)
    }

    if (is.list(run_obj) && !is.null(run_obj$status) && is.list(run_obj$status)) {
      for (nm in c("output7", "output1", "output2", "output3", "output4", "output5", "output6")) {
        st <- run_obj$status[[nm]]
        if (is.list(st) &&
            !is.null(st$path) &&
            length(st$path) == 1L &&
            !is.na(st$path) &&
            nzchar(st$path)) {
          rf <- dirname(as.character(st$path))
          return(rf)
        }
      }
    }

    rf
  }

  run_one_wrapper <- function(f) {
    tryCatch(
      {
        out <- ua_run_end_to_end(
          in_path     = f,
          out_dir     = out_dir,
          location    = location,
          make_weekly = make_weekly,
          overwrite   = overwrite,
          outputs     = outputs
        )
        list(ok = TRUE, out = out, err = NULL)
      },
      error = function(e) list(ok = FALSE, out = NULL, err = conditionMessage(e))
    )
  }

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

      res <- run_one_wrapper(f)

      if (isTRUE(res$ok)) {
        idx[i, status := "ok"]
        idx[i, run_folder := extract_run_folder(res$out)]
      } else {
        idx[i, status := "failed"]
        idx[i, error := as.character(res$err)]
        if (isTRUE(fail_fast)) {
          stop(sprintf("File failed: %s\nReason: %s", basename(f), res$err), call. = FALSE)
        }
      }
    }

    index_path <- file.path(out_dir, sprintf("UA_FOLDER_INDEX_%s.csv", format(Sys.Date(), "%Y-%m-%d")))
    data.table::fwrite(idx, index_path)
    message("[UA] Wrote folder index: ", index_path)

    return(invisible(idx))
  }

  if (!file.exists(in_path_norm)) {
    stop("in_path does not exist: ", in_path_norm, call. = FALSE)
  }
  if (!is_supported(in_path_norm)) {
    stop("Unsupported file type (allowed: csv/txt/xlsx/xls/rds): ", in_path_norm, call. = FALSE)
  }

  out <- ua_run_end_to_end(
    in_path     = in_path_norm,
    out_dir     = out_dir,
    location    = location,
    make_weekly = make_weekly,
    overwrite   = overwrite,
    outputs     = outputs
  )

  idx1 <- data.table(
    input_file = in_path_norm,
    file_name  = basename(in_path_norm),
    status     = "ok",
    error      = NA_character_,
    run_folder = extract_run_folder(out)
  )

  invisible(idx1)
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
                   fail_fast = FALSE,
                   outputs = c("output1", "output2", "output3", "output4", "output5", "output6")) {

  ua_analyze_precomputed(
    in_path     = in_path,
    out_dir     = out_dir,
    location    = location,
    make_weekly = make_weekly,
    overwrite   = overwrite,
    fail_fast   = fail_fast,
    outputs     = outputs
  )
}
