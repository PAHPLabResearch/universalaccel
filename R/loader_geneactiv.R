# R/geneactiv_loader.R
#' Read and calibrate GENEActiv .bin
#'
#' Tries GENEAread::recalibrate() first. On Windows, GENEAread can be
#' sensitive to directory strings, so UA normalizes and forces trailing
#' slashes on datadir/outputdir before calling recalibrate().
#'
#' If recalibration succeeds and a recalibrated .bin is found, that file
#' is read. Otherwise UA falls back to direct read of the original .bin and
#' records the reason in the loader log metadata.
#' @noRd
read_and_calibrate_geneactiv <- function(file_path,
                                         sample_rate    = 100,
                                         tz             = "UTC",
                                         use_header_cal = TRUE,
                                         spherecrit     = 0.3,
                                         minloadcrit    = 24,
                                         chunksize      = 0.5,
                                         windowsizes    = c(5, 900, 3600),
                                         verbose        = FALSE) {
  if (!requireNamespace("GENEAread", quietly = TRUE))
    stop("Package 'GENEAread' not installed.")
  if (!requireNamespace("dplyr", quietly = TRUE))
    stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE))
    stop("Package 'lubridate' is required.")
  if (!requireNamespace("tibble", quietly = TRUE))
    stop("Package 'tibble' is required.")

  file_path <- normalizePath(file_path, winslash = "/", mustWork = TRUE)

  add_trailing_slash_existing <- function(x) {
    x <- normalizePath(x, winslash = "/", mustWork = TRUE)
    if (!grepl("/$", x)) x <- paste0(x, "/")
    x
  }

  add_trailing_slash_any <- function(x) {
    x <- normalizePath(x, winslash = "/", mustWork = FALSE)
    if (!grepl("/$", x)) x <- paste0(x, "/")
    x
  }

  file_dir <- add_trailing_slash_existing(dirname(file_path))

  # temp output dir path does not exist yet, so mustWork must be FALSE here
  out_recal_base <- tempfile("GENEA_recal_dir_")
  dir.create(out_recal_base, recursive = TRUE, showWarnings = FALSE)
  out_recal <- add_trailing_slash_existing(out_recal_base)

  on.exit(unlink(sub("/$", "", out_recal), recursive = TRUE), add = TRUE)

  stem <- tools::file_path_sans_ext(basename(file_path))

  loader_notes <- c(
    paste0("GENEActiv input file: ", basename(file_path)),
    paste0("GENEActiv datadir used for recalibrate(): ", file_dir),
    paste0("GENEActiv outputdir used for recalibrate(): ", out_recal),
    "GENEActiv recalibration was attempted."
  )

  calibration_attempted <- TRUE
  calibration_success <- FALSE
  calibration_note <- "not_attempted"
  calibration_source <- "GENEAread_recalibrate"

  recal_err <- NULL

  tryCatch(
    {
      invisible(
        capture.output(
          GENEAread::recalibrate(
            datadir      = file_dir,
            outputdir    = out_recal,
            use.temp     = TRUE,
            spherecrit   = spherecrit,
            minloadcrit  = minloadcrit,
            printsummary = verbose,
            chunksize    = chunksize,
            windowsizes  = windowsizes
          ),
          type = "output"
        )
      )
    },
    error = function(e) {
      recal_err <<- conditionMessage(e)
    }
  )

  all_out_tmp <- list.files(out_recal, full.names = TRUE, recursive = TRUE)
  all_bin_tmp <- list.files(out_recal, pattern = "\\.bin$", full.names = TRUE, recursive = TRUE)

  loader_notes <- c(
    loader_notes,
    paste0("Files found in recalibration output dir: ", length(all_out_tmp))
  )

  if (length(all_out_tmp)) {
    loader_notes <- c(
      loader_notes,
      paste0("Output dir files: ", paste(basename(all_out_tmp), collapse = "; "))
    )
  } else {
    loader_notes <- c(loader_notes, "No files found in recalibration output dir.")
  }

  target <- NULL

  preferred_tmp <- all_bin_tmp[grepl(paste0("^", stem, "_Recalibrate\\.bin$"), basename(all_bin_tmp))]
  if (length(preferred_tmp)) {
    info <- file.info(preferred_tmp)
    target <- preferred_tmp[which.max(info$mtime)]
    loader_notes <- c(loader_notes, paste0("Selected recalibrated BIN in output dir: ", basename(target)))
  }

  if (is.null(target)) {
    cand_tmp <- all_bin_tmp[grepl(stem, basename(all_bin_tmp), fixed = TRUE)]
    if (length(cand_tmp)) {
      info <- file.info(cand_tmp)
      target <- cand_tmp[which.max(info$mtime)]
      loader_notes <- c(loader_notes, paste0("Selected stem-matched BIN in output dir: ", basename(target)))
    }
  }

  if (is.null(target)) {
    all_bin_src <- list.files(file_dir, pattern = "\\.bin$", full.names = TRUE, recursive = FALSE)
    preferred_src <- all_bin_src[grepl(paste0("^", stem, "_Recalibrate\\.bin$"), basename(all_bin_src))]
    if (length(preferred_src)) {
      info <- file.info(preferred_src)
      target <- preferred_src[which.max(info$mtime)]
      loader_notes <- c(
        loader_notes,
        "No recalibrated BIN found in temp output dir; source folder was inspected.",
        paste0("Selected recalibrated BIN in source dir: ", basename(target))
      )
    }
  }

  if (!is.null(recal_err)) {
    loader_notes <- c(loader_notes, paste0("GENEActiv recalibration error captured: ", recal_err))
  }

  if (!is.null(target) && file.exists(target)) {
    bin <- tryCatch(
      GENEAread::read.bin(target, calibrate = use_header_cal, verbose = FALSE),
      error = function(e) {
        stop("Failed to read recalibrated GENEActiv file '", basename(target),
             "'. Reason: ", conditionMessage(e))
      }
    )

    calibration_success <- TRUE
    calibration_note <- "ok"
    loader_notes <- c(loader_notes, "GENEActiv recalibration output was located and used.")
  } else {
    bin <- tryCatch(
      GENEAread::read.bin(file_path, calibrate = use_header_cal, verbose = FALSE),
      error = function(e) {
        stop("GENEActiv direct read also failed for file '", basename(file_path),
             "'. Reason: ", conditionMessage(e))
      }
    )

    calibration_success <- FALSE
    calibration_note <- if (!is.null(recal_err)) {
      paste0("recalibration_failed_direct_read_used:", recal_err)
    } else {
      "recalibration_output_not_found_direct_read_used"
    }

    loader_notes <- c(
      loader_notes,
      "No usable recalibrated BIN output could be located.",
      "Fallback direct read of the original BIN file was used."
    )
  }

  df <- tibble::tibble(
    time = as.POSIXct(bin$data.out[, "timestamp"], origin = "1970-01-01", tz = tz),
    X    = as.numeric(bin$data.out[, "x"]),
    Y    = as.numeric(bin$data.out[, "y"]),
    Z    = as.numeric(bin$data.out[, "z"])
  ) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  if (!nrow(df)) {
    stop("No samples after GENEActiv read: ", basename(file_path))
  }

  n  <- nrow(df)
  t0 <- lubridate::floor_date(df$time[1], "second")

  out <- tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = df$X,
    Y = df$Y,
    Z = df$Z
  )

  spec <- list(
    model = "geneactiv_bin",
    source_tz_detected = TRUE,
    source_tz = tz,
    compute_tz = tz,
    output_tz = tz,
    tz_rule = "compute_in_source_tz",
    grid_action = "reindexed",
    regularized_for_uniformity = FALSE,
    calibration_input_provenance = "device_loader_native",
    calibration_expected = TRUE,
    calibration_source = calibration_source,
    calibration_attempted = calibration_attempted,
    calibration_success = calibration_success,
    calibration_note = calibration_note,
    units_choice = "g",
    autocalibrated = calibration_success,
    autocalibrate_note = calibration_note
  )

  loader_notes <- c(
    loader_notes,
    "Output timeline was rebuilt to the requested sample_rate by order-preserving reindex.",
    "No X/Y/Z interpolation was performed in UA."
  )

  attr(out, "ua_time_spec") <- spec
  attr(out, "ua_time_report") <- loader_notes

  out
}
