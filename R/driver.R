#' Summarize accelerometer files by device (selectable metrics; classic filenames)
#'
#' @param device "actigraph", "axivity", or "geneactiv"
#' @param data_folder Input folder of native files
#' @param output_folder Output folder for CSVs
#' @param epochs Numeric vector of epoch lengths in seconds
#' @param sample_rate Target Hz for loaders that need a grid
#' @param dynamic_range g-range for MIMS (length-2, e.g., c(-8, 8))
#' @param apply_nonwear Logical; if TRUE, compute Choi flags using PhysicalActivity::wearingMarking()
#'   (epoch-aware via perMinuteCts = 60/epoch), then DROP nonwear rows.
#'   Output keeps `choi_nonwear` column but it is always FALSE (no TRUEs written).
#' @param metrics Character vector of metrics to compute. Any of:
#'   c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM"). Defaults to all.
#' @param tz Timezone for timestamps (passed to loaders)
#' @export
accel_summaries <- function(device, data_folder, output_folder,
                            epochs = 60,
                            sample_rate = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            metrics = c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM"),
                            tz = "UTC") {

  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

  # 1) Loader
  loader <- switch(tolower(device),
                   "actigraph" = read_and_calibrate_actigraph,
                   "axivity"   = read_and_calibrate_axivity,
                   "geneactiv" = read_and_calibrate_geneactiv,
                   stop("Unknown device: ", device)
  )

  # 2) File pattern
  pattern <- switch(tolower(device),
                    "actigraph" = "\\.gt3x$",
                    "axivity"   = "\\.csv(\\.gz)?$",
                    "geneactiv" = "\\.bin$"
  )

  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) { message("[WARN] No input files found"); return(invisible(NULL)) }

  # 3) Metric registry
  metrics  <- toupper(metrics)
  valid    <- c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM")
  unknown  <- setdiff(metrics, valid)
  if (length(unknown)) stop("Unknown metric(s): ", paste(unknown, collapse = ", "))

  run_metric <- list(
    MIMS   = function(d,f,e)  calculate_mims(d, f, e, dynamic_range = dynamic_range),
    AI     = function(d,f,e)  calculate_ai(d, f, e, sample_rate = sample_rate),
    COUNTS = function(d,f,e)  calculate_activity_counts(d, f, e, sample_rate = sample_rate),
    ENMO   = function(d,f,e)  calculate_enmo(d, f, e),
    MAD    = function(d,f,e)  calculate_mad(d, f, e),
    ROCAM  = function(d,f,e)  calculate_rocam(d, f, e)
  )

  # Nonwear helper: epoch-aware using compute_choi_nonwear(), then drop nonwear rows.
  # Guarantee: `choi_nonwear` exists but has no TRUE in final output.
  mark_nonwear_epoch <- function(joined, epoch_sec) {
    if (!requireNamespace("dplyr", quietly = TRUE)) {
      stop("Package 'dplyr' is required for apply_nonwear=TRUE.")
    }

    if (!("Vector.Magnitude" %in% names(joined))) {
      message("  [NONWEAR] Vector.Magnitude not present; skipping nonwear marking")
      joined$choi_nonwear <- FALSE
      return(joined)
    }

    message(sprintf("  [NONWEAR] Choi (PhysicalActivity::wearingMarking) epoch-aware: epoch=%ss", epoch_sec))

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
      streamFrame = NULL,          # mirror package default
      epoch_sec = as.integer(epoch_sec),
      getMinuteMarking = FALSE,    # epoch-by-epoch marking
      tz = tz
    )

    out <- joined |>
      dplyr::left_join(
        dplyr::select(vm_flag, id, time, choi_nonwear),
        by = c("ID" = "id", "time" = "time")
      ) |>
      dplyr::mutate(choi_nonwear = dplyr::coalesce(choi_nonwear, FALSE)) |>
      dplyr::filter(!choi_nonwear) |>
      dplyr::mutate(choi_nonwear = FALSE)

    out
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
      df_cal <- tryCatch(loader(fp, sample_rate = sample_rate, tz = tz),
                         error = function(e) { message("  [ERR] ", e$message); return(NULL) })
      if (is.null(df_cal)) return(NULL)

      # 4) Compute selected metrics
      pieces <- lapply(metrics, function(m) run_metric[[m]](df_cal, fp, epoch))
      names(pieces) <- metrics

      message("  [QC] metric availability:")
      for (nm in names(pieces)) {
        di <- pieces[[nm]]
        if (is.null(di) || !nrow(di)) {
          message(sprintf("    - %-7s: NULL/empty", nm))
        } else {
          message(sprintf("    - %-7s: %d rows", nm, nrow(di)))
        }
      }

      # Require all requested metrics to have rows; otherwise skip this file
      if (any(vapply(pieces, function(x) is.null(x) || !nrow(x), logical(1)))) {
        message("  [SKIP] one or more metrics empty")
        return(NULL)
      }

      # De-dup keys per piece
      pieces <- lapply(pieces, function(d) dplyr::distinct(d, time, ID, .keep_all = TRUE))

      # 5) Inner join across metrics (default behavior)
      joined <- suppressMessages(Reduce(function(x, y) dplyr::inner_join(x, y, by = c("time","ID")), pieces))
      if (!nrow(joined)) { message("  [SKIP] join produced 0 rows"); return(NULL) }

      # 6) Optional Choi non-wear (epoch-aware; then DROP nonwear rows)
      if (apply_nonwear) {
        joined <- mark_nonwear_epoch(joined, epoch_sec = epoch)
        if (!nrow(joined)) { message("  [SKIP] all rows removed as nonwear"); return(NULL) }
      }

      joined |>
        dplyr::distinct(time, ID, .keep_all = TRUE) |>
        dplyr::arrange(time)
    })

    final_df <- dplyr::bind_rows(results)
    if (!nrow(final_df)) { message("  [SKIP] no rows written"); next }

    # Classic filename (unchanged)
    out_file <- file.path(
      output_folder,
      paste0("UA_", tolower(device), "_epoch", epoch, "s_", Sys.Date(), ".csv")
    )
    readr::write_csv(final_df, out_file)
    message("  [WRITE] ", out_file)
  }

  message("\n[DONE] all epochs")
}
