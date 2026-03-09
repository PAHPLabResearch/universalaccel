# R/driver.R
#' Summarize accelerometer sensor data derived from different devices
#'
#' - loads each file via appropriate loader
#' - calls metric calculators (MIMS/AI/COUNTS/ENMO/MAD/ROCAM)
#' - optionally computes COUNTS in background to obtain Vector.Magnitude for Choi nonwear
#' - joins metrics by (time, ID)
#' - writes output CSV per epoch
#' - writes:
#'   * one narrative .txt processing log for the run
#'   * one structured .csv processing summary for the run
#'
#' Time writing policy:
#' - internal computations preserve loader-returned instants
#' - final CSV timestamps are WRITTEN AS CHARACTER in the user-requested `tz`
#' - logs also display timestamps in the user-requested `tz`
#' - timezone conversion for display/export uses lubridate::with_tz()
#'
#' @export
accel_summaries <- function(device, data_folder, output_folder,
                            epochs = 60,
                            sample_rate = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            metrics = c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM"),
                            tz = "UTC",
                            autocalibrate = TRUE,
                            generic_units = c("auto","g","m/s2"),
                            generic_verbose = FALSE) {

  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("readr", quietly = TRUE)) stop("Package 'readr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")

  if (!is.logical(autocalibrate) || length(autocalibrate) != 1 || is.na(autocalibrate)) {
    stop("`autocalibrate` must be TRUE or FALSE.")
  }

  generic_units <- match.arg(generic_units)

  stopifnot(is.character(tz), length(tz) == 1, nzchar(tz))

  tz_ok <- tryCatch({
    x <- as.POSIXct("1970-01-01 00:00:00", tz = tz)
    !is.na(x)
  }, error = function(e) FALSE)

  if (!isTRUE(tz_ok)) stop("Invalid timezone supplied to `tz`: ", tz)

  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
  counts_supported_hz <- function(sr) sr %in% c(30,40,50,60,70,80,90,100)

  force_posix_time <- function(x) {
    if (inherits(x, "POSIXt")) return(x)
    suppressWarnings(as.POSIXct(x, origin = "1970-01-01", tz = "UTC"))
  }

  format_time_in_tz <- function(x, tz_out, fmt = "%Y-%m-%d %H:%M:%OS3") {
    x <- force_posix_time(x)
    if (!length(x)) return(character())
    out <- rep(NA_character_, length(x))
    ok <- !is.na(x)
    if (any(ok)) {
      out[ok] <- format(lubridate::with_tz(x[ok], tzone = tz_out), format = fmt, tz = tz_out)
    }
    out
  }

  pretty_cal_status <- function(spec) {
    src  <- spec$calibration_source %||% ""
    ok   <- isTRUE(spec$calibration_success)
    note <- spec$calibration_note %||% spec$autocalibrate_note %||% ""

    if (ok) {
      if (identical(src, "upstream_resampled_calibrated")) return("yes (upstream)")
      if (grepl("^applied_with_caution:", note)) return("yes (caution)")
      if (grepl("^accepted_with_caution:", note)) return("yes (caution)")
      return("yes")
    }

    if (grepl("^user_disabled", note)) return("no (user disabled)")
    if (grepl("^skipped_due_to_severely_irregular_source_grid", note)) return("no (irregular grid)")
    if (grepl("^skipped_degenerate_signal_variation", note)) return("no (degenerate signal)")
    if (grepl("^failed:", note)) return("no (failed)")
    if (grepl("^failed_all_attempts:", note)) return("no (failed)")
    if (grepl("^agcounts_missing", note)) return("no (agcounts missing)")
    "no"
  }

  prepend_dispatch_note <- function(out, note, dispatch_tag = NULL,
                                    original_device = NULL, source_loader = NULL) {
    rpt <- attr(out, "ua_time_report")
    attr(out, "ua_time_report") <- unique(c(note, rpt))

    spec <- attr(out, "ua_time_spec")
    if (!is.null(spec)) {
      spec$dispatch_note <- note
      if (!is.null(dispatch_tag)) spec$dispatch_tag <- dispatch_tag
      if (!is.null(original_device)) spec$dispatch_original_device_request <- original_device
      if (!is.null(source_loader)) spec$dispatch_source_loader <- source_loader
      attr(out, "ua_time_spec") <- spec
    }
    out
  }

  safe_read_axivity_with_fallback <- function(fp, sample_rate, tz) {
    out <- tryCatch(
      read_and_calibrate_axivity(
        fp,
        sample_rate = sample_rate,
        tz = tz,
        autocalibrate = autocalibrate,
        verbose = generic_verbose
      ),
      error = function(e) {
        msg <- conditionMessage(e)

        if (grepl("^AXIVITY_NON_OMGUI_CSV:", msg)) {
          out2 <- read_and_calibrate_generic(
            fp,
            sample_rate = sample_rate,
            tz = tz,
            autocalibrate = autocalibrate,
            units = generic_units,
            verbose = generic_verbose
          )

          out2 <- prepend_dispatch_note(
            out2,
            note = "Driver dispatch: Axivity CSV did not match OMGUI resampled format; routed automatically to generic loader.",
            dispatch_tag = "axivity_csv_auto_rerouted_to_generic",
            original_device = "axivity",
            source_loader = "generic"
          )

          return(out2)
        }

        stop(e)
      }
    )

    ext0 <- tolower(tools::file_ext(fp))
    if (identical(ext0, "cwa")) {
      out <- prepend_dispatch_note(
        out,
        note = "Driver dispatch: Axivity .cwa detected; routed to Axivity loader.",
        dispatch_tag = "axivity_cwa_native",
        original_device = "axivity",
        source_loader = "axivity"
      )
    } else if (grepl("\\.csv(\\.gz)?$", tolower(basename(fp)))) {
      spec <- attr(out, "ua_time_spec")
      if (!is.null(spec) && identical(spec$model %||% "", "axivity_resampled_csv")) {
        out <- prepend_dispatch_note(
          out,
          note = "Driver dispatch: Axivity CSV matched OMGUI resampled format; routed to Axivity loader.",
          dispatch_tag = "axivity_csv_omgui",
          original_device = "axivity",
          source_loader = "axivity"
        )
      }
    }

    out
  }

  run_start <- Sys.time()
  run_id <- format(run_start, "%Y%m%d_%H%M%S")
  log_txt_path <- file.path(output_folder, paste0("UA_processing_log_", run_id, ".txt"))
  log_csv_path <- file.path(output_folder, paste0("UA_processing_summary_", run_id, ".csv"))

  log_rows <- list()
  file_detail_lines <- character()

  append_detail <- function(...) {
    file_detail_lines <<- c(file_detail_lines, paste0(...))
    invisible(NULL)
  }

  loader <- switch(
    tolower(device),
    "actigraph" = function(fp, sample_rate, tz) {
      read_and_calibrate_actigraph(
        fp,
        sample_rate = sample_rate,
        tz = tz,
        autocalibrate = autocalibrate,
        verbose = generic_verbose
      )
    },
    "axivity" = function(fp, sample_rate, tz) {
      safe_read_axivity_with_fallback(
        fp,
        sample_rate = sample_rate,
        tz = tz
      )
    },
    "geneactiv" = function(fp, sample_rate, tz) {
      read_and_calibrate_geneactiv(
        fp,
        sample_rate = sample_rate,
        tz = tz,
        autocalibrate = autocalibrate,
        verbose = generic_verbose
      )
    },
    "generic" = function(fp, sample_rate, tz) {
      read_and_calibrate_generic(
        fp,
        sample_rate   = sample_rate,
        tz            = tz,
        autocalibrate = autocalibrate,
        units         = generic_units,
        verbose       = generic_verbose
      )
    },
    stop("Unknown device: ", device, ". Valid: actigraph, axivity, geneactiv, generic")
  )

  pattern <- switch(
    tolower(device),
    "actigraph" = "\\.gt3x$",
    "axivity"   = "\\.(cwa|csv|csv\\.gz)$",
    "geneactiv" = "\\.bin$",
    "generic"   = "\\.(csv(\\.gz)?|rds|rda|rdata|xlsx|xls|parquet|feather)$"
  )

  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) {
    message("[WARN] No input files found")
    return(invisible(NULL))
  }

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
    MIMS   = function(d,f,e) calculate_mims(d, f, e, dynamic_range = dynamic_range),
    AI     = function(d,f,e) calculate_ai(d, f, e, sample_rate = sample_rate),
    COUNTS = function(d,f,e) calculate_activity_counts(d, f, e, sample_rate = sample_rate),
    ENMO   = function(d,f,e) calculate_enmo(d, f, e),
    MAD    = function(d,f,e) calculate_mad(d, f, e),
    ROCAM  = function(d,f,e) calculate_rocam(d, f, e)
  )

  mark_nonwear_epoch <- function(joined, epoch_sec) {
    if (!("Vector.Magnitude" %in% names(joined))) {
      joined$choi_nonwear <- FALSE
      return(joined)
    }

    vm_epoch <- joined |>
      dplyr::transmute(
        id = ID,
        time = force_posix_time(time),
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
      dplyr::mutate(time = force_posix_time(time)) |>
      dplyr::left_join(
        dplyr::select(vm_flag, id, time, choi_nonwear),
        by = c("ID" = "id", "time" = "time")
      ) |>
      dplyr::mutate(choi_nonwear = dplyr::coalesce(choi_nonwear, FALSE)) |>
      dplyr::filter(!choi_nonwear) |>
      dplyr::mutate(choi_nonwear = FALSE)
  }

  header_lines <- c(
    "UNIVERSALACCEL PROCESSING LOG",
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
    paste0("  autocalibrate: ", if (isTRUE(autocalibrate)) "TRUE" else "FALSE"),
    paste0("  generic_units: ", generic_units),
    paste0("  generic_verbose: ", if (isTRUE(generic_verbose)) "TRUE" else "FALSE"),
    "",
    "OUTPUT TIME WRITING POLICY",
    paste0("  final CSV timestamps are written as local clock text in tz = ", tz),
    "  logs also display timestamps in the requested tz",
    "  timezone conversion for display/export uses lubridate::with_tz()",
    "  this avoids silent UTC/Z serialization in exported CSV files",
    ""
  )
  writeLines(header_lines, con = log_txt_path)

  make_log_row <- function(epoch_s, file, status, seconds, n_samples,
                           first_time, last_time, spec,
                           counts_bg, nonwear, note = "") {
    data.frame(
      run_id = run_id,
      device = tolower(device),
      epoch_s = epoch_s,
      file = file,
      status = status,
      seconds = seconds,
      n_samples = n_samples,
      first_time = first_time,
      last_time = last_time,
      time_model = spec$model %||% NA_character_,
      source_tz = spec$source_tz %||% NA_character_,
      output_tz = spec$output_tz %||% tz,
      tz_rule = spec$tz_rule %||% NA_character_,
      grid_action = spec$grid_action %||% NA_character_,
      autocal = if (isTRUE(spec$calibration_success)) "yes" else "no",
      autocal_note = spec$calibration_note %||% NA_character_,
      counts_bg = counts_bg,
      nonwear = nonwear,
      note = note,
      stringsAsFactors = FALSE
    )
  }

  empty_spec <- function() {
    list(
      model = NA_character_,
      source_tz = NA_character_,
      output_tz = tz,
      tz_rule = NA_character_,
      grid_action = NA_character_,
      calibration_success = FALSE,
      calibration_note = NA_character_
    )
  }

  for (epoch in epochs) {
    epoch <- as.integer(epoch)
    if (!is.finite(epoch) || epoch <= 0L) stop("Invalid epoch in epochs: ", epoch)
    if ((60L %% epoch) != 0L) stop("Epoch must divide 60 seconds for Choi perMinuteCts; got epoch=", epoch)

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
          log_rows[[length(log_rows) + 1L]] <<- make_log_row(
            epoch_s = epoch,
            file = file_name,
            status = "LOADER_FAIL",
            seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
            n_samples = NA_integer_,
            first_time = NA_character_,
            last_time = NA_character_,
            spec = empty_spec(),
            counts_bg = "no",
            nonwear = if (isTRUE(apply_nonwear)) "requested" else "off",
            note = conditionMessage(e)
          )
          return(NULL)
        }
      )
      if (is.null(df_cal)) return(NULL)

      df_cal$time <- force_posix_time(df_cal$time)
      message("  [LOAD] ok: n=", nrow(df_cal))

      spec <- attr(df_cal, "ua_time_spec")
      rep_lines <- attr(df_cal, "ua_time_report")

      message("  [AUTOCAL] ", pretty_cal_status(spec))

      first_time_chr <- if (nrow(df_cal)) format_time_in_tz(df_cal$time[1], tz_out = tz) else NA_character_
      last_time_chr  <- if (nrow(df_cal)) format_time_in_tz(df_cal$time[nrow(df_cal)], tz_out = tz) else NA_character_

      if (!is.null(rep_lines) && length(rep_lines)) {
        append_detail("")
        append_detail(paste0("FILE DETAIL (epoch=", epoch, "s): ", file_name))
        append_detail(paste0("  first loader timestamp: ", first_time_chr))
        append_detail(paste0("  last loader timestamp : ", last_time_chr))
        append_detail(paste0("  time model            : ", spec$model %||% NA_character_))
        append_detail(paste0("  source_tz_detected    : ", if (isTRUE(spec$source_tz_detected)) "yes" else "no"))
        append_detail(paste0("  source_tz             : ", spec$source_tz %||% NA_character_))
        append_detail(paste0("  compute_tz            : ", spec$compute_tz %||% NA_character_))
        append_detail(paste0("  output_tz             : ", spec$output_tz %||% tz))
        append_detail(paste0("  tz_rule               : ", spec$tz_rule %||% NA_character_))
        append_detail(paste0("  grid_action           : ", spec$grid_action %||% NA_character_))
        append_detail(paste0("  regularized_for_grid  : ", if (isTRUE(spec$regularized_for_uniformity)) "yes" else "no"))
        append_detail(paste0("  grid_note             : ", spec$grid_regularization_note %||% NA_character_))
        append_detail(paste0("  cal_input_provenance  : ", spec$calibration_input_provenance %||% NA_character_))
        append_detail(paste0("  calibration_source    : ", spec$calibration_source %||% NA_character_))
        append_detail(paste0("  calibration_attempted : ", if (isTRUE(spec$calibration_attempted)) "yes" else "no"))
        append_detail(paste0("  calibration_success   : ", if (isTRUE(spec$calibration_success)) "yes" else "no"))
        append_detail(paste0("  calibration_note      : ", spec$calibration_note %||% NA_character_))
        append_detail(paste0("  units_choice          : ", spec$units_choice %||% NA_character_))
        if (!is.null(spec$dispatch_note)) {
          append_detail(paste0("  dispatch_note         : ", spec$dispatch_note))
        }
        append_detail("  loader notes:")
        append_detail(paste0("    - ", rep_lines))
      }

      pieces <- lapply(metrics_run, function(m) {
        tryCatch(run_metric[[m]](df_cal, fp, epoch), error = function(e) NULL)
      })
      names(pieces) <- metrics_run

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
        log_rows[[length(log_rows) + 1L]] <<- make_log_row(
          epoch_s = epoch,
          file = file_name,
          status = "METRIC_FAIL",
          seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
          n_samples = nrow(df_cal),
          first_time = first_time_chr,
          last_time = last_time_chr,
          spec = spec,
          counts_bg = counts_bg_status,
          nonwear = if (isTRUE(apply_nonwear)) "requested" else "off",
          note = paste0("Empty metric(s): ", paste(empties, collapse = ", "))
        )
        return(NULL)
      }

      pieces_for_join <- pieces
      if (used_counts_bg) pieces_for_join <- c(pieces_for_join, list(COUNTS_BG = counts_bg))

      pieces_for_join <- lapply(pieces_for_join, function(d) {
        d$time <- force_posix_time(d$time)
        dplyr::distinct(d, time, ID, .keep_all = TRUE)
      })

      joined <- suppressMessages(
        Reduce(function(x, y) dplyr::inner_join(x, y, by = c("time", "ID")), pieces_for_join)
      )

      if (!nrow(joined)) {
        f_end <- Sys.time()
        message("  [SKIP] join produced 0 rows")
        log_rows[[length(log_rows) + 1L]] <<- make_log_row(
          epoch_s = epoch,
          file = file_name,
          status = "JOIN_ZERO",
          seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
          n_samples = nrow(df_cal),
          first_time = first_time_chr,
          last_time = last_time_chr,
          spec = spec,
          counts_bg = counts_bg_status,
          nonwear = if (isTRUE(apply_nonwear)) "requested" else "off",
          note = "Inner join produced 0 rows"
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
          log_rows[[length(log_rows) + 1L]] <<- make_log_row(
            epoch_s = epoch,
            file = file_name,
            status = "NONWEAR_ALL",
            seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
            n_samples = nrow(df_cal),
            first_time = first_time_chr,
            last_time = last_time_chr,
            spec = spec,
            counts_bg = counts_bg_status,
            nonwear = "applied",
            note = "All rows removed as nonwear"
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
        drop_cols <- intersect(
          names(joined),
          c("Axis1", "Axis2", "Axis3", "Steps", "Vector.Magnitude",
            "Lux", "Inclination", "Temperature")
        )
        joined <- dplyr::select(joined, -dplyr::any_of(drop_cols))
      }

      joined$time <- force_posix_time(joined$time)

      f_end <- Sys.time()
      log_rows[[length(log_rows) + 1L]] <<- make_log_row(
        epoch_s = epoch,
        file = file_name,
        status = "OK",
        seconds = round(as.numeric(difftime(f_end, f_start, units = "secs")), 3),
        n_samples = nrow(df_cal),
        first_time = first_time_chr,
        last_time = last_time_chr,
        spec = spec,
        counts_bg = counts_bg_status,
        nonwear = nonwear_status,
        note = ""
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

    final_df$time <- force_posix_time(final_df$time)

    final_df_out <- final_df |>
      dplyr::mutate(time = format_time_in_tz(time, tz_out = tz))

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
    append_detail("  note                   : final CSV timestamps written as local clock text after explicit with_tz() conversion")
    append_detail(paste0("  epoch time (sec)       : ", round(as.numeric(difftime(epoch_end, epoch_start, units = "secs")), 3)))
  }

  run_end <- Sys.time()
  elapsed_total <- round(as.numeric(difftime(run_end, run_start, units = "secs")), 3)
  log_df <- if (length(log_rows)) do.call(rbind, log_rows) else data.frame()

  if (length(file_detail_lines)) {
    write(c("", "DETAILED FILE NOTES", file_detail_lines), file = log_txt_path, append = TRUE)
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
  write(summary_lines, file = log_txt_path, append = TRUE)

  glossary_lines <- c(
    "ABBREVIATIONS & EXPLANATIONS",
    "",
    "  first_time / last_time",
    "    - first and last timestamps returned by the loader, displayed in the requested tz.",
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
    "",
    "  output_tz",
    paste0("    - the timezone used for final CSV writing. In this run: ", tz),
    "",
    "  tz_rule",
    "    - compute_in_source_tz         : compute timeline preserved source instant behavior",
    "    - compute_in_assumed_source_tz : naive timestamps interpreted in assumed source timezone",
    "",
    "  grid_action",
    "    - kept      : original timeline already matched the requested strict grid",
    "    - snapped   : timestamps rounded to nearest grid tick; duplicate ticks averaged",
    "    - reindexed : strict time index rebuilt preserving sample order only; no X/Y/Z interpolation",
    "",
    "  autocal",
    "    - yes/no indicates whether calibration succeeded for that file in the loader context",
    "",
    "  autocal_note",
    "    - ok                               : calibration succeeded",
    "    - already_calibrated_upstream      : upstream-calibrated file accepted as calibrated",
    "    - skipped_due_to_severely_irregular_source_grid",
    "                                      : file was too irregular for safe autocalibration even after regularization",
    "    - skipped_degenerate_signal_variation",
    "                                      : file signal variation was too degenerate for safe calibration",
    "    - accepted_with_caution:*          : calibration output had minor structural deviation within tolerance",
    "    - rejected:*                       : calibration output failed structural guardrails",
    "    - failed:*                         : calibration call errored and UA fell back to uncalibrated data",
    "    - failed_all_attempts:*            : all calibration attempts failed or were rejected",
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
    "  2) Regularization is applied only when the file is not already on a strict requested grid.",
    "  3) For mostly near-target files, UA may regularize time by snap/collapse and, if needed, sample-order reindexing.",
    "  4) Autocalibration is allowed on best-effort regularized grids only when the file passes the near-target threshold.",
    "  5) Final CSV timestamps are written as character in the requested tz, not UTC Z strings.",
    "  6) Time display/export uses explicit with_tz() conversion.",
    "  7) For device='axivity', non-OMGUI CSV files are automatically rerouted to the generic loader.",
    ""
  )
  write(glossary_lines, file = log_txt_path, append = TRUE)

  if (nrow(log_df)) {
    readr::write_csv(log_df, log_csv_path)
  } else {
    readr::write_csv(data.frame(), log_csv_path)
  }

  message("[LOG] wrote: ", log_txt_path)
  message("[LOG] wrote: ", log_csv_path)

  invisible(list(
    output_folder = output_folder,
    log_file_txt = log_txt_path,
    log_file_csv = log_csv_path
  ))
}
