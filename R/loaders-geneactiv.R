#' Read + autocalibrate GENEActiv, with demo-friendly fallback
#' @noRd
read_and_calibrate_geneactiv <- function(file_path,
                                         sample_rate = 100,
                                         tz = "UTC",
                                         recal_dir   = file.path(dirname(file_path), "recalibrated"),
                                         use.temp    = TRUE,
                                         spherecrit  = 0.3,
                                         minloadcrit = 24,
                                         chunksize   = 0.5,
                                         windowsizes = c(5, 900, 3600),
                                         verbose     = FALSE) {
  
  size_kb <- tryCatch(as.numeric(file.info(file_path)$size) / 1024, error = function(e) NA_real_)
  use_direct <- is.na(size_kb) || size_kb < 1024  # demo-ish → read directly
  
  if (use_direct) {
    if (verbose) message("[GENEA] small file → read.bin(calibrate=TRUE)")
    bin <- GENEAread::read.bin(file_path, calibrate = TRUE, verbose = FALSE)
  } else {
    if (!dir.exists(recal_dir)) dir.create(recal_dir, recursive = TRUE)
    base   <- basename(file_path)
    target <- file.path(recal_dir, base)
    
    if (!file.exists(target)) {
      if (verbose) message("[GENEA] recalibrate → ", base)
      GENEAread::recalibrate(
        datadir      = dirname(file_path),
        outputdir    = recal_dir,
        use.temp     = use.temp,
        spherecrit   = spherecrit,
        minloadcrit  = minloadcrit,
        printsummary = verbose,
        chunksize    = chunksize,
        windowsizes  = windowsizes
      )
      if (!file.exists(target)) {
        stem <- tools::file_path_sans_ext(base)
        cand <- list.files(recal_dir, pattern = paste0("^", stem, ".*\\.bin$"), full.names = TRUE)
        if (length(cand)) {
          info <- file.info(cand); target <- cand[which.max(info$mtime)]
        }
      }
      if (!file.exists(target)) stop("Calibrated file not found in ", recal_dir)
    }
    bin <- GENEAread::read.bin(target, calibrate = TRUE, verbose = FALSE)
  }
  
  df <- tibble::tibble(
    time = as.POSIXct(bin$data.out[, "timestamp"], origin = "1970-01-01", tz = tz),
    X    = as.numeric(bin$data.out[, "x"]),
    Y    = as.numeric(bin$data.out[, "y"]),
    Z    = as.numeric(bin$data.out[, "z"])
  ) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)
  
  if (!nrow(df)) stop("No samples after read: ", basename(file_path))
  
  # infer sample rate if not given
  if (is.null(sample_rate)) {
    dt <- diff(as.numeric(df$time)); dt <- dt[is.finite(dt) & dt > 0]
    sample_rate <- if (length(dt)) as.integer(round(1 / stats::median(dt))) else 100L
  }
  
  n  <- nrow(df)
  t0 <- lubridate::floor_date(df$time[1], "second")
  tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = df$X, Y = df$Y, Z = df$Z
  )
}
