# R/driver.R
#' Summarize accelerometer sensor data derived from different devices
#'
#' - loads each file via appropriate loader
#' - calls metric calculators (MIMS/AI/COUNTS/ENMO/MAD/ROCAM)
#' - optionally computes COUNTS in background to obtain Vector.Magnitude for Choi nonwear
#' - joins metrics by (time, ID)
#' - writes output CSV per epoch
#' - writes one professional .txt log for the run
#'
#' Time writing policy:
#' - final CSV timestamps are WRITTEN AS CHARACTER in the user-requested `tz`
#' - this avoids silent UTC/Z serialization on export
#'
#' @param device "actigraph", "axivity", "geneactiv", or "generic"
#' @param data_folder Input folder of native files
#' @param output_folder Output folder for CSVs
#' @param epochs Numeric vector of epoch lengths in seconds
#' @param sample_rate Target Hz for loaders that need a grid
#' @param dynamic_range g-range for MIMS (length-2, e.g., c(-8, 8))
#' @param apply_nonwear Logical; if TRUE, compute Choi flags (requires Vector.Magnitude).
#'   For generic runs, if Vector.Magnitude is not present because COUNTS wasn't requested,
#'   UA will attempt to compute COUNTS internally (only if Hz supports it) to obtain VM.
#' @param metrics Character vector of metrics to compute. Any of:
#'   c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM"). Defaults to all.
#' @param tz Timezone for timestamps (passed to loaders and used for final CSV writing)
#' @param generic_autocalibrate For device="generic": "auto","true","false"
#' @param generic_units For device="generic": "auto","g","m/s2"
#' @param generic_verbose For device="generic": extra messages
#' @export
accel_summaries <- function(device, data_folder, output_folder,
                            epochs = 60,
                            sample_rate = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            metrics = c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM"),
                            tz = "UTC",
                            generic_autocalibrate = c("auto","true","false"),
                            generic_units = c("auto","g","m/s2"),
                            generic_verbose = FALSE) {

  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("readr", quietly = TRUE)) stop("Package 'readr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")

  generic_autocalibrate <- match.arg(tolower(generic_autocalibrate),
                                     choices = c("auto","true","false"))
  generic_units <- match.arg(generic_units)

  stopifnot(is.character(tz), length(tz) == 1, nzchar(tz))

  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

  counts_supported_hz <- function(sr) sr %in% c(30,40,50,60,70,80,90,100)

  # ---------------------------------------------------------------------------
  # log helpers
  # ---------------------------------------------------------------------------
  run_start <- Sys.time()
  run_id <- format(run_start, "%Y%m%d_%H%M%S")
  log_path <- file.path(output_folder, paste0("UA_summarypreprocessing_Log_", run_id, ".txt"))
  log_rows <- list()
  file_detail_lines <- character()

  fmt_cell <- function(x, w) {
    x <- if (is.null(x) || length(x) == 0 || is.na(x)) "" else as.character(x)
    x <- gsub("[\r\n\t]+", " ", x)
    if (nchar(x) > w) x <- paste0(substr(x, 1, w - 3), "...")
    sprintf(paste0("%-", w, "s"), x)
  }

  make_table <- function(df, widths) {
    stopifnot(is.data.frame(df))
    cols <- names(df)
    if (length(widths) != length(cols)) stop("widths must match number of columns")
    header <- paste(mapply(fmt_cell, cols, widths), collapse = " | ")
    sep <- paste(mapply(function(x) paste(rep("-", x), collapse = ""), widths), collapse = "-+-")
    body <- apply(df, 1, function(row) paste(mapply(fmt_cell, row, widths), collapse = " | "))
    c(header, sep, body)
  }

  append_detail <- function(...) {
    file_detail_lines <<- c(file_detail_lines, paste0(...))
    invisible(NULL)
  }

  # ---------------------------------------------------------------------------
  # loader selection
  # ---------------------------------------------------------------------------
  loader <- switch(tolower(device),
                   "actigraph" = read_and_calibrate_actigraph,
                   "axivity"   = read_and_calibrate_axivity,
                   "geneactiv" = read_and_calibrate_geneactiv,
                   "generic"   = function(fp, sample_rate, tz) {
                     read_and_calibrate_generic(
                       fp,
                       sample_rate   = sample_rate,
                       tz            = tz,
                       autocalibrate = generic_autocalibrate,
                       units         = generic_units,
                       verbose       = generic_verbose
                     )
                   },
                   stop("Unknown device: ", device,
                        ". Valid: actigraph, axivity, geneactiv, generic")
  )

  # ---------------------------------------------------------------------------
  # file pattern
  # ---------------------------------------------------------------------------
  pattern <- switch(tolower(device),
                    "actigraph" = "\\.gt3x$",
                    "axivity"   = "\\.csv(\\.gz)?$",
                    "geneactiv" = "\\.bin$",
                    "generic"   = "\\.(csv(\\.gz)?|rds|rda|rdata|xlsx|xls|parquet|feather)$"
  )

  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) {
    message("[WARN] No input files found")
    return(invisible(NULL))
  }

  # ---------------------------------------------------------------------------
  # metric registry
  # ---------------------------------------------------------------------------
  metrics_in <- toupper(metrics)
  valid <- c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM")
  unknown <- setdiff(metrics_in, valid)
  if (length(unknown)) stop("Unknown metric(s): ", paste(unknown, collapse = ", "))

  user_requested_counts <- "COUNTS" %in% metrics_in

  metrics_run <- metrics_in
  if ("COUNTS" %in% metrics_run && !counts_supported_hz(sample_rate)) {
    message(
      "[NOTE] COUNTS requested at sample_rate=", sample_rate, "Hz, but COUNTS supports only: ",
      "30–100 Hz (30,40,50,60,70,80,90,100). Dropping COUNTS.\n",
      "       Other metrics will still run."
    )
    metrics_run <- setdiff(metrics_run, "COUNTS")
  }

  run_metric <- list(
    MIMS   = function(d,f,e)  calculate_mims(d, f, e, dynamic_range = dynamic_range),
    AI     = function(d,f,e)  calculate_ai(d, f, e, sample_rate = sample_rate),
    COUNTS = function(d,f,e)  calculate_activity_counts(d, f, e, sample_rate = sample_rate),
    ENMO   = function(d,f,e)  calculate_enmo(d, f, e),
    MAD    = function(d,f,e)  calculate_mad(d, f, e),
    ROCAM  = function(d,f,e)  calculate_rocam(d, f, e)
  )

  # ---------------------------------------------------------------------------
  # nonwear helper
  # ---------------------------------------------------------------------------
  mark_nonwear_epoch <- function(joined, epoch_sec) {
    if (!("Vector.Magnitude" %in% names(joined))) {
      joined$choi_nonwear <- FALSE
      return(joined)
    }

    vm_epoch <- joined |>
      dplyr::transmute(
        id = ID,
        time = time,
        vector_magnitude = Vector.Magnitude
      ) |>
      dplyr::distinct(id, time, .keep_all = TRUE) |>
      dplyr::arrange(id, time)

    vm_flag <- compute_choi_nonwear(
      vm_epoch,
      id_col = "id",
      time_col = "time",
      cpm_col = "vector_magnitude",
      frame = 90L,
      allowance = 2L,
      streamFrame = NULL,
      epoch_sec = as.integer(epoch_sec),
      getMinuteMarking = FALSE,
      tz = tz
    )

    joined |>
      dplyr::left_join(
        dplyr::select(vm_flag, id, time, choi_nonwear),
        by = c("ID" = "id", "time" = "time")
      ) |>
      dplyr::mutate(choi_nonwear = dplyr::coalesce(choi_nonwear, FALSE)) |>
      dplyr::filter(!choi_nonwear) |>
      dplyr::mutate(choi_nonwear = FALSE)
  }

  # ---------------------------------------------------------------------------
  # write log header
  # ---------------------------------------------------------------------------
  header_lines <- c(
    "UNIVERSALACCEL SUMMARY PREPROCESSING LOG",
    paste0("Run ID: ", run_id),
    paste0("Run started: ", format(run_start, "%Y-%m-%d %H:%M:%S")),
    "",
    "RUN SETTINGS",
    paste0("  device: ", tolower(device)),
    paste0("  data_folder: ", normalizePath(data_folder, winslash = "/", mustWork = FALSE)),
    paste0("  output_folder: ", normalizePath(output_folder, winslash = "/", mustWork = FALSE)),
    paste0("  tz requested: ", tz),
    paste0("  sample_rate (Hz): ", sample_rate),
    paste0("  epochs (s): ", paste(as.integer(epochs), collapse = ", ")),
    paste0("  metrics requested: ", paste(metrics_in, collapse = ", ")),
    paste0("  metrics run: ", paste(metrics_run, collapse = ", ")),
    paste0("  apply_nonwear: ", if (isTRUE(apply_nonwear)) "TRUE" else "FALSE"),
    paste0("  generic_autocalibrate: ", generic_autocalibrate),
    paste0("  generic_units: ", generic_units),
    paste0("  generic_verbose: ", if (isTRUE(generic_verbose)) "TRUE" else "FALSE"),
    "",
    "OUTPUT TIME WRITING POLICY",
    paste0("  final CSV timestamps are written as local clock text in tz = ", tz),
    "  this avoids silent UTC/Z serialization in exported CSV files",
    ""
  )
  writeLines(header_lines, con = log_path)

  # ---------------------------------------------------------------------------
  # main epoch loop
  # ---------------------------------------------------------------------------
  for (epoch in epochs) {
    epoch <- as.integer(epoch)
    if (!is.finite(epoch) || epoch <= 0L) stop("Invalid epoch in epochs: ", epoch)
    if ((60L %% epoch) != 0L) {
      stop("Epoch must divide 60 seconds for Choi perMinuteCts; got epoch=", epoch)
    }

    message(paste0("\n[RUN] device=", device, " epoch=", epoch, "s"))
    epoch_start <- Sys.time()

    results <- lapply(files, function(fp) {
      f_start <- Sys.time()
      file_name <- basename(fp)

      message("[FILE] ", file_name)

      df_cal <- tryCatch(
        loader(fp, sample_rate = sample_rate, tz = tz),
        error = function(e) {
          f_end <- Sys.time()
          message("  [ERR] Loader failed: ", file_name)
          message("        reason: ", conditionMessage(e))
          log_rows[[length(log_rows) + 1L]] <<- data.frame(
            epoch_s = epoch,
            file = file_name,
            status = "LOADER_FAIL",
            seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
            n_samples = NA_integer_,
            first_time = NA_character_,
            last_time = NA_character_,
            time_model = NA_character_,
            source_tz = NA_character_,
            output_tz = tz,
            tz_rule = NA_character_,
            grid_action = NA_character_,
            units = NA_character_,
            autocal = NA_character_,
            counts_bg = "no",
            nonwear = if (isTRUE(apply_nonwear)) "requested" else "off",
            note = conditionMessage(e),
            stringsAsFactors = FALSE
          )
          return(NULL)
        }
      )
      if (is.null(df_cal)) return(NULL)

      message("  [LOAD] ok: n=", nrow(df_cal))

      spec <- attr(df_cal, "ua_time_spec")
      rep_lines <- attr(df_cal, "ua_time_report")

      first_time_chr <- if (nrow(df_cal)) format(df_cal$time[1], tz = tz, format = "%Y-%m-%d %H:%M:%OS3") else NA_character_
      last_time_chr  <- if (nrow(df_cal)) format(df_cal$time[nrow(df_cal)], tz = tz, format = "%Y-%m-%d %H:%M:%OS3") else NA_character_

      # detailed per-file section
      if (!is.null(rep_lines) && length(rep_lines)) {
        append_detail("")
        append_detail(paste0("FILE DETAIL (epoch=", epoch, "s): ", file_name))
        append_detail(paste0("  first loader timestamp: ", first_time_chr))
        append_detail(paste0("  last loader timestamp : ", last_time_chr))
        append_detail(paste0("  time model            : ", spec$model %||% NA_character_))
        append_detail(paste0("  source_tz_detected    : ", if (isTRUE(spec$source_tz_detected)) "yes" else "no"))
        append_detail(paste0("  source_tz             : ", spec$source_tz %||% NA_character_))
        append_detail(paste0("  output_tz             : ", spec$output_tz %||% tz))
        append_detail(paste0("  tz_rule               : ", spec$tz_rule %||% NA_character_))
        append_detail(paste0("  grid_action           : ", spec$grid_action %||% NA_character_))
        append_detail(paste0("  units_choice          : ", spec$units_choice %||% NA_character_))
        append_detail(paste0("  autocalibrated        : ", if (isTRUE(spec$autocalibrated)) "yes" else "no"))
        append_detail("  loader notes:")
        append_detail(paste0("    - ", rep_lines))
      }

      # requested metrics
      pieces <- lapply(metrics_run, function(m) {
        tryCatch(run_metric[[m]](df_cal, fp, epoch),
                 error = function(e) NULL)
      })
      names(pieces) <- metrics_run

      # background counts for nonwear
      counts_bg <- NULL
      used_counts_bg <- FALSE
      counts_bg_status <- "no"

      if (isTRUE(apply_nonwear)) {
        if (!("COUNTS" %in% metrics_run)) {
          if (!counts_supported_hz(sample_rate)) {
            counts_bg_status <- "skipped_hz"
          } else {
            counts_bg_status <- "attempted"
            counts_bg <- tryCatch(
              run_metric[["COUNTS"]](df_cal, fp, epoch),
              error = function(e) NULL
            )
            if (!is.null(counts_bg) && nrow(counts_bg)) {
              used_counts_bg <- TRUE
              counts_bg_status <- "yes"
            } else {
              counts_bg_status <- "failed"
            }
          }
        }
      }

      message("  [QC] metric availability:")
      for (nm in names(pieces)) {
        di <- pieces[[nm]]
        if (is.null(di) || !nrow(di)) {
          message(sprintf("    - %-7s: NULL/empty", nm))
        } else {
          message(sprintf("    - %-7s: %d rows", nm, nrow(di)))
        }
      }
      if (!is.null(counts_bg)) {
        message(sprintf("    - %-10s: %s",
                        "COUNTS(bg)",
                        if (nrow(counts_bg)) paste0(nrow(counts_bg), " rows") else "NULL/empty"))
      }

      empties <- names(pieces)[vapply(pieces, function(x) is.null(x) || !nrow(x), logical(1))]
      if (length(empties)) {
        f_end <- Sys.time()
        message("  [SKIP] ", file_name, " skipped because metric(s) failed/empty: ",
                paste(empties, collapse = ", "))
        log_rows[[length(log_rows) + 1L]] <<- data.frame(
          epoch_s = epoch,
          file = file_name,
          status = "METRIC_FAIL",
          seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
          n_samples = nrow(df_cal),
          first_time = first_time_chr,
          last_time = last_time_chr,
          time_model = spec$model %||% NA_character_,
          source_tz = spec$source_tz %||% NA_character_,
          output_tz = spec$output_tz %||% tz,
          tz_rule = spec$tz_rule %||% NA_character_,
          grid_action = spec$grid_action %||% NA_character_,
          units = spec$units_choice %||% NA_character_,
          autocal = if (isTRUE(spec$autocalibrated)) "yes" else "no",
          counts_bg = counts_bg_status,
          nonwear = if (isTRUE(apply_nonwear)) "requested" else "off",
          note = paste0("Empty metric(s): ", paste(empties, collapse = ", ")),
          stringsAsFactors = FALSE
        )
        return(NULL)
      }

      pieces_for_join <- pieces
      if (used_counts_bg) pieces_for_join <- c(pieces_for_join, list(COUNTS_BG = counts_bg))

      pieces_for_join <- lapply(pieces_for_join, function(d) dplyr::distinct(d, time, ID, .keep_all = TRUE))

      joined <- suppressMessages(Reduce(function(x, y) dplyr::inner_join(x, y, by = c("time", "ID")), pieces_for_join))
      if (!nrow(joined)) {
        f_end <- Sys.time()
        message("  [SKIP] join produced 0 rows")
        log_rows[[length(log_rows) + 1L]] <<- data.frame(
          epoch_s = epoch,
          file = file_name,
          status = "JOIN_ZERO",
          seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
          n_samples = nrow(df_cal),
          first_time = first_time_chr,
          last_time = last_time_chr,
          time_model = spec$model %||% NA_character_,
          source_tz = spec$source_tz %||% NA_character_,
          output_tz = spec$output_tz %||% tz,
          tz_rule = spec$tz_rule %||% NA_character_,
          grid_action = spec$grid_action %||% NA_character_,
          units = spec$units_choice %||% NA_character_,
          autocal = if (isTRUE(spec$autocalibrated)) "yes" else "no",
          counts_bg = counts_bg_status,
          nonwear = if (isTRUE(apply_nonwear)) "requested" else "off",
          note = "Inner join produced 0 rows",
          stringsAsFactors = FALSE
        )
        return(NULL)
      }

      nonwear_status <- "off"
      if (isTRUE(apply_nonwear) && ("Vector.Magnitude" %in% names(joined))) {
        nonwear_status <- "applied"
        message("  [NONWEAR] Choi: epoch=", epoch, "s")
        joined <- mark_nonwear_epoch(joined, epoch_sec = epoch)
        if (!nrow(joined)) {
          f_end <- Sys.time()
          message("  [SKIP] all rows removed as nonwear")
          log_rows[[length(log_rows) + 1L]] <<- data.frame(
            epoch_s = epoch,
            file = file_name,
            status = "NONWEAR_ALL",
            seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
            n_samples = nrow(df_cal),
            first_time = first_time_chr,
            last_time = last_time_chr,
            time_model = spec$model %||% NA_character_,
            source_tz = spec$source_tz %||% NA_character_,
            output_tz = spec$output_tz %||% tz,
            tz_rule = spec$tz_rule %||% NA_character_,
            grid_action = spec$grid_action %||% NA_character_,
            units = spec$units_choice %||% NA_character_,
            autocal = if (isTRUE(spec$autocalibrated)) "yes" else "no",
            counts_bg = counts_bg_status,
            nonwear = "applied",
            note = "All rows removed as nonwear",
            stringsAsFactors = FALSE
          )
          return(NULL)
        }
      } else if (isTRUE(apply_nonwear) && !("Vector.Magnitude" %in% names(joined))) {
        nonwear_status <- "skipped_no_vm"
        message("  [NONWEAR] apply_nonwear=TRUE but Vector.Magnitude unavailable; skipping nonwear marking")
      } else if (isTRUE(apply_nonwear)) {
        nonwear_status <- "requested"
      }

      if (used_counts_bg && !user_requested_counts) {
        drop_cols <- intersect(names(joined),
                               c("Axis1", "Axis2", "Axis3", "Steps", "Vector.Magnitude",
                                 "Lux", "Inclination", "Temperature"))
        joined <- dplyr::select(joined, -dplyr::any_of(drop_cols))
      }

      f_end <- Sys.time()
      log_rows[[length(log_rows) + 1L]] <<- data.frame(
        epoch_s = epoch,
        file = file_name,
        status = "OK",
        seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
        n_samples = nrow(df_cal),
        first_time = first_time_chr,
        last_time = last_time_chr,
        time_model = spec$model %||% NA_character_,
        source_tz = spec$source_tz %||% NA_character_,
        output_tz = spec$output_tz %||% tz,
        tz_rule = spec$tz_rule %||% NA_character_,
        grid_action = spec$grid_action %||% NA_character_,
        units = spec$units_choice %||% NA_character_,
        autocal = if (isTRUE(spec$autocalibrated)) "yes" else "no",
        counts_bg = counts_bg_status,
        nonwear = nonwear_status,
        note = "",
        stringsAsFactors = FALSE
      )

      message("  [DONE] ", file_name, " -> ", nrow(joined), " rows")

      joined |>
        dplyr::distinct(time, ID, .keep_all = TRUE) |>
        dplyr::arrange(time)
    })

    final_df <- dplyr::bind_rows(results)

    if (!nrow(final_df)) {
      message("  [SKIP] no rows written for epoch=", epoch)
      epoch_end <- Sys.time()
      append_detail("")
      append_detail(paste0("EPOCH SUMMARY: ", epoch, "s"))
      append_detail("  status: no output written")
      append_detail(paste0("  epoch time (sec): ", round(as.numeric(difftime(epoch_end, epoch_start, units = "secs")), 3)))
      next
    }

    # IMPORTANT: write time as character in requested tz, not UTC/Z
    final_df_out <- final_df |>
      dplyr::mutate(
        time = format(
          as.POSIXct(time, tz = tz),
          tz = tz,
          format = "%Y-%m-%d %H:%M:%OS3"
        )
      )

    out_file <- file.path(
      output_folder,
      paste0("UA_", tolower(device), "_epoch", epoch, "s_", Sys.Date(), ".csv")
    )
    readr::write_csv(final_df_out, out_file)
    message("  [WRITE] ", out_file)

    epoch_end <- Sys.time()
    append_detail("")
    append_detail(paste0("EPOCH SUMMARY: ", epoch, "s"))
    append_detail(paste0("  output file            : ", basename(out_file)))
    append_detail(paste0("  rows written           : ", nrow(final_df_out)))
    append_detail(paste0("  time written in tz     : ", tz))
    append_detail("  time output format     : %Y-%m-%d %H:%M:%OS3")
    append_detail("  note                   : final CSV timestamps written as local clock text, not UTC ISO8601 Z")
    append_detail(paste0("  epoch time (sec)       : ", round(as.numeric(difftime(epoch_end, epoch_start, units = "secs")), 3)))
  }

  # ---------------------------------------------------------------------------
  # final log writing
  # ---------------------------------------------------------------------------
  run_end <- Sys.time()
  elapsed_total <- round(as.numeric(difftime(run_end, run_start, units = "secs")), 3)

  log_df <- if (length(log_rows)) do.call(rbind, log_rows) else data.frame()

  write(
    x = c("",
          "FILE RESULTS TABLE (one row per file per epoch)",
          "Columns: epoch_s, file, status, seconds, n_samples, first_time, last_time, time_model, source_tz, output_tz, tz_rule, grid_action, units, autocal, counts_bg, nonwear, note"),
    file = log_path,
    append = TRUE
  )

  if (nrow(log_df)) {
    table_lines <- make_table(
      log_df,
      widths = c(
        7,
        28,
        12,
        8,
        10,
        23,
        23,
        16,
        18,
        18,
        12,
        10,
        7,
        8,
        10,
        12,
        28
      )
    )
    write(table_lines, file = log_path, append = TRUE)
  } else {
    write("No files produced log rows.", file = log_path, append = TRUE)
  }

  if (length(file_detail_lines)) {
    write(c("", "DETAILED FILE NOTES", file_detail_lines), file = log_path, append = TRUE)
  }

  ok_n <- if (nrow(log_df)) sum(log_df$status == "OK", na.rm = TRUE) else 0L
  fail_n <- if (nrow(log_df)) sum(log_df$status != "OK", na.rm = TRUE) else 0L

  summary_lines <- c(
    "",
    "BATCH SUMMARY",
    paste0("  run ended: ", format(run_end, "%Y-%m-%d %H:%M:%S")),
    paste0("  total runtime (sec): ", elapsed_total),
    paste0("  files x epochs processed OK: ", ok_n),
    paste0("  files x epochs with issues: ", fail_n),
    ""
  )
  write(summary_lines, file = log_path, append = TRUE)

  glossary_lines <- c(
    "ABBREVIATIONS & EXPLANATIONS",
    "",
    "  first_time / last_time",
    "    - first and last timestamps returned by the loader for that file, shown in the requested tz.",
    "",
    "  time_model",
    "    - posix                  : time was already POSIX-like",
    "    - epoch_s/ms/us/ns       : numeric Unix epoch time",
    "    - excel                  : Excel serial date",
    "    - datetime_with_offset   : raw strings explicitly contained timezone/offset",
    "    - datetime_string_local  : raw strings did not encode timezone; interpreted in requested tz",
    "    - epoch_plus_elapsed     : absolute anchor + elapsed ticks reconstruction",
    "",
    "  source_tz",
    "    - the timezone source UA inferred for interpreting raw timestamps.",
    "    - examples: encoded_in_raw, UTC-origin instant, assumed_America/Chicago",
    "",
    "  output_tz",
    paste0("    - the timezone used for final CSV writing. In this run: ", tz),
    "",
    "  tz_rule",
    "    - with_tz     : raw time was treated as an instant, then expressed in output_tz",
    "    - local_parse : raw naive datetime strings were interpreted directly in output_tz",
    "",
    "  grid_action (NO interpolation)",
    "    - kept     : original timeline retained",
    "    - snapped  : timestamps rounded to nearest grid tick; duplicate ticks averaged",
    "    - rebuilt  : strict time index rebuilt preserving sample order",
    "",
    "  autocal",
    "    - yes/no indicates whether autocalibration succeeded in the loader",
    "",
    "  counts_bg",
    "    - yes        : COUNTS computed internally only to obtain Vector.Magnitude for nonwear",
    "    - attempted  : background COUNTS attempted but empty/NULL",
    "    - failed     : background COUNTS could not be computed",
    "    - skipped_hz : background COUNTS not supported at this sample rate",
    "    - no         : not needed",
    "",
    "  nonwear",
    "    - applied        : Choi nonwear was computed and applied",
    "    - skipped_no_vm  : nonwear requested but Vector.Magnitude unavailable",
    "    - requested/off  : user setting state",
    "",
    "IMPORTANT NOTES",
    "  1) UA does NOT interpolate X/Y/Z.",
    "  2) Final CSV timestamps are written as character in the requested tz, not UTC Z strings.",
    "  3) If source_tz was assumed rather than detected from raw data, verify it matches device/export settings.",
    ""
  )
  write(glossary_lines, file = log_path, append = TRUE)

  message("[LOG] wrote: ", log_path)

  invisible(list(output_folder = output_folder, log_file = log_path))
}
