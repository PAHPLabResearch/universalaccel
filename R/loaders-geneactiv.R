# R/geneactiv_loader.R
#' @noRd
# Calibrate + read GENEActiv .bin in one go, with safe fallback for tiny demo files
read_and_calibrate_geneactiv <- function(file_path,
                                         sample_rate   = 100,
                                         tz            = "UTC",
                                         use_header_cal= TRUE,
                                         spherecrit    = 0.3,
                                         minloadcrit   = 24,
                                         chunksize     = 0.5,
                                         windowsizes   = c(5, 900, 3600),
                                         verbose       = FALSE) {
  if (!requireNamespace("GENEAread", quietly = TRUE))
    stop("Package GENEAread not installed.")

  file_path <- normalizePath(file_path, winslash = "/", mustWork = TRUE)
  size_mb   <- as.numeric(file.info(file_path)$size) / (1024^2)

  # ---- FAST PATH for tiny demo files: skip recalibration, just read.bin() ----
  if (!is.na(size_mb) && size_mb < 1) {
    if (verbose) message("[GENEA] demo-sized file (", sprintf("%.1f MB", size_mb),
                         ") -> skipping recalibration; using header calibration only")
    bin <- GENEAread::read.bin(file_path, calibrate = use_header_cal, verbose = FALSE)
    return(.genea_bin_to_tbl(bin, sample_rate, tz))
  }

  # ---- Normal path: recalibrate into a temp folder, then read the calibrated copy ----
  out_recal <- tempfile("GENEA_recal_")
  dir.create(out_recal, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(out_recal, recursive = TRUE), add = TRUE)

  # Try recalibrate; if it fails (short file, etc.), fall back to direct read
  ok <- try({
    GENEAread::recalibrate(
      datadir      = dirname(file_path),
      outputdir    = out_recal,
      use.temp     = TRUE,
      spherecrit   = spherecrit,
      minloadcrit  = minloadcrit,
      printsummary = verbose,
      chunksize    = chunksize,
      windowsizes  = windowsizes
    )
    TRUE
  }, silent = TRUE)

  target <- NULL
  if (isTRUE(ok)) {
    stem <- tools::file_path_sans_ext(basename(file_path))
    cand <- list.files(out_recal, pattern = paste0("^", stem, ".*\\.bin$"), full.names = TRUE)
    if (length(cand)) {
      info   <- file.info(cand)
      target <- cand[which.max(info$mtime)]  # freshest
    }
  }

  # If no calibrated file appeared, fall back to direct read
  if (is.null(target) || !file.exists(target)) {
    if (verbose) message("[GENEA] recalibration unavailable -> using header calibration directly")
    bin <- GENEAread::read.bin(file_path, calibrate = use_header_cal, verbose = FALSE)
    return(.genea_bin_to_tbl(bin, sample_rate, tz))
  }

  # Otherwise read the calibrated copy
  bin <- GENEAread::read.bin(target, calibrate = use_header_cal, verbose = FALSE)
  .genea_bin_to_tbl(bin, sample_rate, tz)
}

# helper to convert GENEAread bin to tibble with aligned time grid
.genea_bin_to_tbl <- function(bin, sample_rate, tz) {
  df <- tibble::tibble(
    time = as.POSIXct(bin$data.out[, "timestamp"], origin = "1970-01-01", tz = tz),
    X    = as.numeric(bin$data.out[, "x"]),
    Y    = as.numeric(bin$data.out[, "y"]),
    Z    = as.numeric(bin$data.out[, "z"])
  ) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  if (!nrow(df)) stop("No samples after read: ", utils::tail(bin$header$file.name, 1))

  n  <- nrow(df)
  t0 <- lubridate::floor_date(df$time[1], "second")
  tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = df$X, Y = df$Y, Z = df$Z
  )
}
