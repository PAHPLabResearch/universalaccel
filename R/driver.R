# R/accel_summaries.R
#' Summarize accelerometer sensor data derived from different devices
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
#' @param tz Timezone for timestamps (passed to loaders)
#' @param generic_autocalibrate For device="generic": "auto","true","false"
#' @param generic_units For device="generic": "auto","g","m/s2"
#' @param generic_verbose For device="generic": extra messages
#' @export
# R/accel_summaries.R
#'
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

  generic_autocalibrate <- match.arg(tolower(generic_autocalibrate),
                                     choices = c("auto","true","false"))
  generic_units <- match.arg(generic_units)

  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

  counts_supported_hz <- function(sr) sr %in% c(30,40,50,60,70,80,90,100)

  # 1) Loader
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

  # 2) File pattern
  pattern <- switch(tolower(device),
                    "actigraph" = "\\.gt3x$",
                    "axivity"   = "\\.csv(\\.gz)?$",
                    "geneactiv" = "\\.bin$",
                    "generic"   = "\\.(csv(\\.gz)?|rds|rda|rdata|xlsx|xls|parquet|feather)$"
  )

  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) { message("[WARN] No input files found"); return(invisible(NULL)) }

  # 3) Metric registry
  metrics_in <- toupper(metrics)
  valid <- c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM")
  unknown <- setdiff(metrics_in, valid)
  if (length(unknown)) stop("Unknown metric(s): ", paste(unknown, collapse = ", "))

  # Track explicit COUNTS request BEFORE potentially dropping it
  user_requested_counts <- "COUNTS" %in% metrics_in

  # Drop COUNTS if Hz unsupported
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

  mark_nonwear_epoch <- function(joined, epoch_sec) {
    if (!requireNamespace("dplyr", quietly = TRUE)) {
      stop("Package 'dplyr' is required for apply_nonwear=TRUE.")
    }

    if (!("Vector.Magnitude" %in% names(joined))) {
      message("  [NONWEAR] Vector.Magnitude not present; skipping nonwear marking")
      joined$choi_nonwear <- FALSE
      return(joined)
    }

    message(sprintf("[NONWEAR] Choi: epoch=%ss", epoch_sec))

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

  for (epoch in epochs) {
    epoch <- as.integer(epoch)
    if (!is.finite(epoch) || epoch <= 0L) stop("Invalid epoch in epochs: ", epoch)
    if ((60L %% epoch) != 0L) {
      stop("Epoch must divide 60 seconds for Choi perMinuteCts; got epoch=", epoch)
    }

    message(paste0("\n[RUN] device=", device, " epoch=", epoch, "s"))

    results <- lapply(files, function(fp) {
      message("[FILE] ", basename(fp))

      df_cal <- tryCatch(
        loader(fp, sample_rate = sample_rate, tz = tz),
        error = function(e) {
          message("  [ERR] Loader failed: ", basename(fp))
          message("        device=", device, " sample_rate=", sample_rate, "Hz tz=", tz)
          message("        reason: ", conditionMessage(e))
          return(NULL)
        }
      )
      if (is.null(df_cal)) return(NULL)

      # 4) Compute requested metrics (excluding COUNTS if dropped)
      pieces <- lapply(metrics_run, function(m) {
        tryCatch(run_metric[[m]](df_cal, fp, epoch),
                 error = function(e) {
                   message("  [ERR] Metric ", m, " failed for ", basename(fp))
                   message("        reason: ", conditionMessage(e))
                   if (m == "COUNTS" && !counts_supported_hz(sample_rate)) {
                     message("        Tip: COUNTS supports only 30–100 Hz; remove 'COUNTS' or use supported sample_rate.")
                   }
                   if (m == "AI") {
                     message("        Tip: AI requires complete per-second samples; irregular seconds can cause failure.")
                   }
                   NULL
                 })
      })
      names(pieces) <- metrics_run

      # 4b) If apply_nonwear=TRUE, ensure Vector.Magnitude exists:
      # If COUNTS wasn't run, attempt it in background (Hz permitting).
      counts_bg <- NULL
      used_counts_bg <- FALSE

      if (isTRUE(apply_nonwear)) {
        if (!("COUNTS" %in% metrics_run)) {
          if (!counts_supported_hz(sample_rate)) {
            message(
              "[NOTE] apply_nonwear=TRUE requested, but Vector.Magnitude requires COUNTS and ",
              "COUNTS supports only 30–100 Hz. sample_rate=", sample_rate, "Hz.\n",
              "       Nonwear marking will be SKIPPED for this file."
            )
          } else {
            message("[NONWEAR] apply_nonwear=TRUE: computing COUNTS internally to obtain Vector.Magnitude")
            counts_bg <- tryCatch(run_metric[["COUNTS"]](df_cal, fp, epoch),
                                  error = function(e) {
                                    message("  [NONWEAR] COUNTS(bg) failed; cannot compute Vector.Magnitude.")
                                    message("           reason: ", conditionMessage(e))
                                    NULL
                                  })
            if (!is.null(counts_bg) && nrow(counts_bg)) used_counts_bg <- TRUE
          }
        }
      }

      # QC availability
      message("  [QC] metric availability:")
      for (nm in names(pieces)) {
        di <- pieces[[nm]]
        if (is.null(di) || !nrow(di)) message(sprintf("    - %-7s: NULL/empty", nm))
        else message(sprintf("    - %-7s: %d rows", nm, nrow(di)))
      }
      if (!is.null(counts_bg)) {
        message(sprintf("    - %-10s: %s", "COUNTS(bg)", if (nrow(counts_bg)) paste0(nrow(counts_bg), " rows") else "NULL/empty"))
      }

      # If any requested metric failed/empty -> skip (your design)
      empties <- names(pieces)[vapply(pieces, function(x) is.null(x) || !nrow(x), logical(1))]
      if (length(empties)) {
        message("  [SKIP] ", basename(fp), " skipped because metric(s) failed/empty: ",
                paste(empties, collapse = ", "))
        return(NULL)
      }

      # Join set (requested + optional bg counts)
      pieces_for_join <- pieces
      if (used_counts_bg) {
        pieces_for_join <- c(pieces_for_join, list(COUNTS_BG = counts_bg))
      }

      # De-dup keys per piece
      pieces_for_join <- lapply(pieces_for_join, function(d) dplyr::distinct(d, time, ID, .keep_all = TRUE))

      joined <- suppressMessages(Reduce(function(x, y) dplyr::inner_join(x, y, by = c("time","ID")), pieces_for_join))
      if (!nrow(joined)) { message("  [SKIP] join produced 0 rows"); return(NULL) }

      # Optional nonwear (only if we actually have Vector.Magnitude)
      if (isTRUE(apply_nonwear) && ("Vector.Magnitude" %in% names(joined))) {
        joined <- mark_nonwear_epoch(joined, epoch_sec = epoch)
        if (!nrow(joined)) { message("  [SKIP] all rows removed as nonwear"); return(NULL) }
      } else if (isTRUE(apply_nonwear) && !("Vector.Magnitude" %in% names(joined))) {
        message("  [NONWEAR] apply_nonwear=TRUE but Vector.Magnitude unavailable; skipping nonwear marking")
      }

      # If COUNTS was only computed in background, remove its extra columns unless user *requested* COUNTS
      if (used_counts_bg && !user_requested_counts) {
        drop_cols <- intersect(names(joined),
                               c("Axis1","Axis2","Axis3","Steps","Vector.Magnitude",
                                 "Lux","Inclination","Temperature"))
        joined <- dplyr::select(joined, -dplyr::any_of(drop_cols))
      }

      joined |>
        dplyr::distinct(time, ID, .keep_all = TRUE) |>
        dplyr::arrange(time)
    })

    final_df <- dplyr::bind_rows(results)
    if (!nrow(final_df)) { message("  [SKIP] no rows written"); next }

    out_file <- file.path(
      output_folder,
      paste0("UA_", tolower(device), "_epoch", epoch, "s_", Sys.Date(), ".csv")
    )
    readr::write_csv(final_df, out_file)
    message("  [WRITE] ", out_file)
  }

  message("\n[DONE] all epochs")
}
