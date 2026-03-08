# R/geneactiv_loader.R
#' Read and optionally recalibrate GENEActiv .bin
#'
#' Design:
#' - native BIN read via GENEAread::read.bin()
#' - optional recalibration via GENEAread::recalibrate()
#' - user controls recalibration with a Boolean flag
#' - recalibration failure never kills the whole file if direct read works
#' - strict monotonic / uniform timeline for downstream metrics
#' - no interpolation of X/Y/Z
#'
#' Attached attributes:
#' - attr(out, "ua_time_spec")
#' - attr(out, "ua_time_report")
#'
#' @noRd
read_and_calibrate_geneactiv <- function(file_path,
                                         sample_rate    = 100,
                                         tz             = "UTC",
                                         autocalibrate  = TRUE,
                                         use_header_cal = TRUE,
                                         spherecrit     = 0.3,
                                         minloadcrit    = 24,
                                         chunksize      = 0.5,
                                         windowsizes    = c(5, 900, 3600),
                                         verbose        = FALSE) {
  if (!is.logical(autocalibrate) || length(autocalibrate) != 1 || is.na(autocalibrate)) {
    stop("`autocalibrate` must be TRUE or FALSE.")
  }

  if (!requireNamespace("GENEAread", quietly = TRUE))
    stop("Package 'GENEAread' not installed.")
  if (!requireNamespace("dplyr", quietly = TRUE))
    stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE))
    stop("Package 'lubridate' is required.")
  if (!requireNamespace("tibble", quietly = TRUE))
    stop("Package 'tibble' is required.")

  stopifnot(is.finite(sample_rate), sample_rate > 0)
  stopifnot(is.character(tz), length(tz) == 1, nzchar(tz))

  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

  loader_notes <- character()

  add_report <- function(...) {
    loader_notes <<- c(loader_notes, paste0(...))
    invisible(NULL)
  }

  add_trailing_slash_existing <- function(x) {
    x <- normalizePath(x, winslash = "/", mustWork = TRUE)
    if (!grepl("/$", x)) x <- paste0(x, "/")
    x
  }

  is_regular_strict <- function(tt, hz, tol = NULL) {
    dt <- 1 / hz
    if (is.null(tol)) tol <- max(1e-6, 0.05 * dt)
    x <- as.numeric(tt)
    if (length(x) < 2L || anyNA(x)) return(FALSE)
    d <- diff(x)
    if (!length(d) || any(!is.finite(d))) return(FALSE)
    all(abs(d - dt) <= tol)
  }

  reindex_strict_by_order <- function(df, hz, compute_tz) {
    df <- df[order(df$time), , drop = FALSE]
    n <- nrow(df)
    if (!n) return(df)
    t0 <- as.POSIXct(df$time[1], tz = compute_tz)
    attr(t0, "tzone") <- compute_tz
    new_time <- t0 + seq(0, by = 1 / hz, length.out = n)
    attr(new_time, "tzone") <- compute_tz
    df$time <- new_time
    df
  }

  rephase_to_whole_second <- function(df, hz, compute_tz, anchor = c("nearest", "floor", "ceiling")) {
    anchor <- match.arg(anchor)
    df <- df[order(df$time), , drop = FALSE]
    n <- nrow(df)
    if (!n) return(list(df = df, phase_shift_sec = 0))

    dt <- 1 / hz
    x <- as.numeric(as.POSIXct(df$time, tz = compute_tz))
    t1 <- x[1]

    target <- switch(
      anchor,
      floor   = floor(t1),
      ceiling = ceiling(t1),
      nearest = round(t1)
    )

    phase_shift_sec <- target - t1
    x2 <- (t1 + phase_shift_sec) + seq(0, by = dt, length.out = n)

    df$time <- as.POSIXct(x2, origin = "1970-01-01", tz = compute_tz)
    attr(df$time, "tzone") <- compute_tz

    list(df = df, phase_shift_sec = phase_shift_sec)
  }

  canonicalize_final_blocks <- function(df, hz, compute_tz, align_to_whole_second = TRUE) {
    n0 <- nrow(df)
    if (!n0) {
      return(list(df = df, n_drop_tail = 0L, phase_shift_sec = 0))
    }

    dt <- 1 / hz
    tt <- as.POSIXct(df$time, tz = compute_tz)
    x <- as.numeric(tt)

    if (length(x) < 2L || anyNA(x)) {
      stop("GENEActiv final canonicalization failed: invalid or too-short time vector.")
    }

    d <- diff(x)
    d <- d[is.finite(d)]
    if (!length(d)) {
      stop("GENEActiv final canonicalization failed: could not compute time differences.")
    }

    if (sum(duplicated(tt)) > 0) {
      stop("GENEActiv final canonicalization failed: duplicated timestamps remained.")
    }
    if (sum(d <= 0) > 0) {
      stop("GENEActiv final canonicalization failed: nonpositive time differences remained.")
    }

    med_dt <- stats::median(d)
    tol <- max(1e-6, 0.05 * dt)
    if (!is.finite(med_dt) || abs(med_dt - dt) > tol) {
      stop(sprintf(
        "GENEActiv final canonicalization failed: median_dt=%.9f did not match expected dt=%.9f.",
        med_dt, dt
      ))
    }

    n_keep <- floor(n0 / hz) * hz
    n_drop_tail <- n0 - n_keep
    if (!is.finite(n_keep) || n_keep <= 0) {
      stop("GENEActiv final canonicalization left fewer than one complete second.")
    }

    out <- df[seq_len(n_keep), , drop = FALSE]
    out$time <- as.POSIXct(out$time, tz = compute_tz)
    attr(out$time, "tzone") <- compute_tz

    phase_shift_sec <- 0
    if (isTRUE(align_to_whole_second)) {
      rp <- rephase_to_whole_second(out, hz = hz, compute_tz = compute_tz, anchor = "nearest")
      out <- rp$df
      phase_shift_sec <- rp$phase_shift_sec
    }

    list(
      df = out,
      n_drop_tail = as.integer(n_drop_tail),
      phase_shift_sec = phase_shift_sec
    )
  }

  summarize_stream <- function(df, label, target_hz, tz_use) {
    out <- list(
      label = label, n = nrow(df), first = NA_character_, last = NA_character_,
      n_dup_time = NA_integer_, n_nonpos_dt = NA_integer_, median_dt = NA_real_,
      duration_sec = NA_real_, approx_hz = NA_real_, vm_med = NA_real_
    )

    if (!nrow(df) || !all(c("time", "X", "Y", "Z") %in% names(df))) return(out)

    tt <- suppressWarnings(as.POSIXct(df$time, tz = tz_use))
    tt_num <- as.numeric(tt)

    out$first <- format(tt[1], tz = tz_use, usetz = TRUE)
    out$last  <- format(tt[nrow(df)], tz = tz_use, usetz = TRUE)
    out$n_dup_time <- sum(duplicated(tt))

    if (length(tt_num) > 1) {
      d <- diff(tt_num)
      d <- d[is.finite(d)]
      if (length(d)) {
        out$n_nonpos_dt <- sum(d <= 0)
        out$median_dt <- stats::median(d)
        out$duration_sec <- tt_num[length(tt_num)] - tt_num[1]
        if (is.finite(out$median_dt) && out$median_dt > 0) out$approx_hz <- 1 / out$median_dt
      }
    }

    vm <- sqrt(as.numeric(df$X)^2 + as.numeric(df$Y)^2 + as.numeric(df$Z)^2)
    out$vm_med <- stats::median(vm[is.finite(vm)], na.rm = TRUE)
    out
  }

  report_stream_summary <- function(ss) {
    sprintf(
      "%s: n=%s; first=%s; last=%s; dup_time=%s; nonpos_dt=%s; median_dt=%.9f; approx_hz=%.6f; duration_sec=%.3f; vm_med=%.6f",
      ss$label, ss$n, ss$first %||% NA_character_, ss$last %||% NA_character_,
      ss$n_dup_time, ss$n_nonpos_dt,
      ifelse(is.finite(ss$median_dt), ss$median_dt, NA_real_),
      ifelse(is.finite(ss$approx_hz), ss$approx_hz, NA_real_),
      ifelse(is.finite(ss$duration_sec), ss$duration_sec, NA_real_),
      ifelse(is.finite(ss$vm_med), ss$vm_med, NA_real_)
    )
  }

  file_path <- normalizePath(file_path, winslash = "/", mustWork = TRUE)
  file_dir  <- add_trailing_slash_existing(dirname(file_path))
  stem      <- tools::file_path_sans_ext(basename(file_path))

  add_report(paste0("GENEActiv input file: ", basename(file_path)))
  add_report(sprintf("Autocalibration requested by user: %s.", if (isTRUE(autocalibrate)) "YES" else "NO"))
  add_report(paste0("GENEActiv read.bin calibrate flag (header calibration): ", use_header_cal))

  target <- NULL
  recal_err <- NULL
  calibration_attempted <- FALSE
  calibration_success <- FALSE
  calibration_note <- "user_disabled"
  calibration_source <- "GENEAread_recalibrate"
  read_source <- "original_bin"

  if (isTRUE(autocalibrate)) {
    calibration_attempted <- TRUE
    calibration_note <- "not_attempted"

    out_recal_base <- tempfile("GENEA_recal_dir_")
    dir.create(out_recal_base, recursive = TRUE, showWarnings = FALSE)
    out_recal <- add_trailing_slash_existing(out_recal_base)
    on.exit(unlink(sub("/$", "", out_recal), recursive = TRUE), add = TRUE)

    add_report(paste0("GENEActiv datadir used for recalibrate(): ", file_dir))
    add_report(paste0("GENEActiv outputdir used for recalibrate(): ", out_recal))
    add_report("GENEActiv recalibration was attempted.")

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

    add_report(paste0("Files found in recalibration output dir: ", length(all_out_tmp)))

    if (length(all_out_tmp)) {
      add_report(paste0("Output dir files: ", paste(basename(all_out_tmp), collapse = "; ")))
    } else {
      add_report("No files found in recalibration output dir.")
    }

    preferred_tmp <- all_bin_tmp[grepl(paste0("^", stem, "_Recalibrate\\.bin$"), basename(all_bin_tmp))]
    if (length(preferred_tmp)) {
      info <- file.info(preferred_tmp)
      target <- preferred_tmp[which.max(info$mtime)]
      add_report(paste0("Selected recalibrated BIN in output dir: ", basename(target)))
    }

    if (is.null(target)) {
      cand_tmp <- all_bin_tmp[grepl(stem, basename(all_bin_tmp), fixed = TRUE)]
      if (length(cand_tmp)) {
        info <- file.info(cand_tmp)
        target <- cand_tmp[which.max(info$mtime)]
        add_report(paste0("Selected stem-matched BIN in output dir: ", basename(target)))
      }
    }

    if (is.null(target)) {
      all_bin_src <- list.files(file_dir, pattern = "\\.bin$", full.names = TRUE, recursive = FALSE)
      preferred_src <- all_bin_src[grepl(paste0("^", stem, "_Recalibrate\\.bin$"), basename(all_bin_src))]
      if (length(preferred_src)) {
        info <- file.info(preferred_src)
        target <- preferred_src[which.max(info$mtime)]
        add_report("No recalibrated BIN found in temp output dir; source folder was inspected.")
        add_report(paste0("Selected recalibrated BIN in source dir: ", basename(target)))
      }
    }

    if (!is.null(recal_err)) {
      add_report(paste0("GENEActiv recalibration error captured: ", recal_err))
    }
  } else {
    add_report("GENEActiv recalibration was skipped by user.")
  }

  if (isTRUE(autocalibrate) && !is.null(target) && file.exists(target)) {
    bin <- tryCatch(
      GENEAread::read.bin(target, calibrate = use_header_cal, verbose = FALSE),
      error = function(e) {
        stop("Failed to read recalibrated GENEActiv file '", basename(target),
             "'. Reason: ", conditionMessage(e))
      }
    )

    calibration_success <- TRUE
    calibration_note <- "ok"
    read_source <- "recalibrated_bin"
    add_report("GENEActiv recalibration output was located and used.")
  } else {
    bin <- tryCatch(
      GENEAread::read.bin(file_path, calibrate = use_header_cal, verbose = FALSE),
      error = function(e) {
        stop("GENEActiv direct read also failed for file '", basename(file_path),
             "'. Reason: ", conditionMessage(e))
      }
    )

    if (isTRUE(autocalibrate)) {
      calibration_success <- FALSE
      calibration_note <- if (!is.null(recal_err)) {
        paste0("recalibration_failed_direct_read_used:", recal_err)
      } else {
        "recalibration_output_not_found_direct_read_used"
      }

      add_report("No usable recalibrated BIN output could be located.")
      add_report("Fallback direct read of the original BIN file was used.")
    } else {
      calibration_success <- FALSE
      calibration_note <- "user_disabled"
      add_report("Direct read of the original BIN file was used.")
    }
  }

  df <- tibble::tibble(
    time = as.POSIXct(bin$data.out[, "timestamp"], origin = "1970-01-01", tz = tz),
    X    = as.numeric(bin$data.out[, "x"]),
    Y    = as.numeric(bin$data.out[, "y"]),
    Z    = as.numeric(bin$data.out[, "z"])
  ) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  attr(df$time, "tzone") <- tz

  if (!nrow(df)) {
    stop("No samples after GENEActiv read: ", basename(file_path))
  }

  raw_summary <- summarize_stream(df, "geneactiv_read", target_hz = sample_rate, tz_use = tz)
  add_report(report_stream_summary(raw_summary))

  if (!is_regular_strict(df$time, hz = sample_rate)) {
    add_report(sprintf(
      "Input timeline was not strict at %g Hz. Rebuilding strict grid by sample order.",
      sample_rate
    ))
    df <- reindex_strict_by_order(df, hz = sample_rate, compute_tz = tz)
  } else {
    add_report(sprintf("Input timeline already matched strict %g Hz.", sample_rate))
  }

  attr(df$time, "tzone") <- tz

  canon <- canonicalize_final_blocks(
    df,
    hz = sample_rate,
    compute_tz = tz,
    align_to_whole_second = TRUE
  )

  out <- tibble::tibble(
    time = as.POSIXct(canon$df$time, tz = tz),
    X = as.numeric(canon$df$X),
    Y = as.numeric(canon$df$Y),
    Z = as.numeric(canon$df$Z)
  )
  attr(out$time, "tzone") <- tz

  if (!nrow(out)) {
    stop("No samples after final GENEActiv canonicalization: ", basename(file_path))
  }

  if (canon$n_drop_tail > 0L) {
    add_report(sprintf(
      "Final canonicalization dropped %d trailing row(s) to retain complete whole-second blocks.",
      canon$n_drop_tail
    ))
  } else {
    add_report("Final canonicalization dropped 0 trailing rows.")
  }

  if (isTRUE(abs(canon$phase_shift_sec) > 0)) {
    add_report(sprintf(
      paste0(
        "Final wall-clock rephasing applied: shifted timestamps by %.6f sec so the stream ",
        "starts on an exact whole-second boundary. X/Y/Z values were unchanged."
      ),
      canon$phase_shift_sec
    ))
  } else {
    add_report("Final wall-clock rephasing check: no shift needed.")
  }

  final_summary <- summarize_stream(out, "final_output", target_hz = sample_rate, tz_use = tz)
  add_report(report_stream_summary(final_summary))
  add_report("No X/Y/Z interpolation was performed in UA.")

  spec <- list(
    model = "geneactiv_bin",
    source_tz_detected = TRUE,
    source_tz = tz,
    compute_tz = tz,
    output_tz = tz,
    tz_rule = "compute_in_source_tz",
    grid_action = "reindexed",
    regularized_for_uniformity = TRUE,
    calibration_input_provenance = "device_loader_native",
    calibration_expected = TRUE,
    calibration_source = calibration_source,
    calibration_attempted = calibration_attempted,
    calibration_success = calibration_success,
    calibration_note = calibration_note,
    units_choice = "g",
    autocalibrated = calibration_success,
    autocalibrate_note = calibration_note,
    calibration_stream_used = read_source,
    final_canonicalization_applied = canon$n_drop_tail > 0L,
    final_canonicalization_head_dropped = 0L,
    final_canonicalization_tail_dropped = canon$n_drop_tail,
    final_wallclock_rephased = isTRUE(abs(canon$phase_shift_sec) > 0),
    final_wallclock_phase_shift_sec = canon$phase_shift_sec
  )

  attr(out, "ua_time_spec") <- spec
  attr(out, "ua_time_report") <- unique(loader_notes)

  out
}
