# R/actigraph_loader.R
#' Read + calibrate ActiGraph GT3X with robust agread fallback behavior
#'
#' Design:
#' - primary autocalibration/read path uses agcounts::agread()
#' - preferred parser order is GGIR -> read.gt3x -> optional pygt3x
#' - if all calibrated read attempts fail, fall back to native read.gt3x
#' - strict monotonic / uniform timeline for downstream metrics
#' - no interpolation of X/Y/Z
#' - calibration failure never kills the whole file
#'
#' Attached attributes:
#' - attr(out, "ua_time_spec")
#' - attr(out, "ua_time_report")
#'
#' @noRd
read_and_calibrate_actigraph <- function(file_path,
                                         sample_rate = 100,
                                         tz = "UTC",
                                         autocalibrate = TRUE,
                                         parser_fallback = c("GGIR", "read.gt3x", "pygt3x"),
                                         verbose = FALSE) {

  if (!is.logical(autocalibrate) || length(autocalibrate) != 1 || is.na(autocalibrate)) {
    stop("`autocalibrate` must be TRUE or FALSE.")
  }

  if (!requireNamespace("read.gt3x", quietly = TRUE)) stop("Package 'read.gt3x' is required.")
  if (!requireNamespace("agcounts", quietly = TRUE)) stop("Package 'agcounts' is required.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")
  if (!requireNamespace("tibble", quietly = TRUE)) stop("Package 'tibble' is required.")

  stopifnot(is.finite(sample_rate), sample_rate > 0)
  stopifnot(is.character(tz), length(tz) == 1, nzchar(tz))

  parser_fallback <- unique(as.character(parser_fallback))
  valid_parsers <- c("GGIR", "read.gt3x", "pygt3x")
  bad_parsers <- setdiff(parser_fallback, valid_parsers)
  if (length(bad_parsers)) {
    stop("Unknown parser_fallback value(s): ", paste(bad_parsers, collapse = ", "))
  }
  if (!length(parser_fallback)) {
    stop("`parser_fallback` must contain at least one parser.")
  }

  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

  .time_report <- character()

  add_report <- function(...) {
    .time_report <<- c(.time_report, paste0(...))
    invisible(NULL)
  }

  vmsg <- function(...) {
    if (isTRUE(verbose)) message(...)
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
      stop("ActiGraph final canonicalization failed: invalid or too-short time vector.")
    }

    d <- diff(x)
    d <- d[is.finite(d)]
    if (!length(d)) {
      stop("ActiGraph final canonicalization failed: could not compute time differences.")
    }

    if (sum(duplicated(tt)) > 0) {
      stop("ActiGraph final canonicalization failed: duplicated timestamps remained.")
    }
    if (sum(d <= 0) > 0) {
      stop("ActiGraph final canonicalization failed: nonpositive time differences remained.")
    }

    med_dt <- stats::median(d)
    tol <- max(1e-6, 0.05 * dt)
    if (!is.finite(med_dt) || abs(med_dt - dt) > tol) {
      stop(sprintf(
        "ActiGraph final canonicalization failed: median_dt=%.9f did not match expected dt=%.9f.",
        med_dt, dt
      ))
    }

    n_keep <- floor(n0 / hz) * hz
    n_drop_tail <- n0 - n_keep
    if (!is.finite(n_keep) || n_keep <= 0) {
      stop("ActiGraph final canonicalization left fewer than one complete second.")
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

  normalize_agread_output <- function(obj, compute_tz) {
    if (!inherits(obj, "data.frame")) obj <- as.data.frame(obj)

    need <- c("time", "X", "Y", "Z")
    if (!all(need %in% names(obj))) {
      stop("agread output did not contain required columns: time, X, Y, Z")
    }

    out <- tibble::tibble(
      time = as.POSIXct(obj$time, tz = compute_tz),
      X = as.numeric(obj$X),
      Y = as.numeric(obj$Y),
      Z = as.numeric(obj$Z)
    ) |>
      dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
      dplyr::arrange(time)

    attr(out$time, "tzone") <- compute_tz
    out
  }

  try_agread <- function(path, parser, compute_tz) {
    err <- NULL
    dat <- tryCatch(
      agcounts::agread(
        path = path,
        parser = parser,
        tz = compute_tz,
        verbose = FALSE
      ),
      error = function(e) {
        err <<- e
        NULL
      }
    )
    list(data = dat, err = err)
  }

  read_native_gt3x <- function(path, compute_tz) {
    raw0 <- read.gt3x::read.gt3x(path, asDataFrame = TRUE, imputeZeroes = FALSE)

    if (!inherits(raw0$time, "POSIXt")) {
      raw0$time <- as.POSIXct(raw0$time, tz = compute_tz)
    }

    raw <- raw0 |>
      dplyr::transmute(
        time = as.POSIXct(time, tz = compute_tz),
        X = as.numeric(X),
        Y = as.numeric(Y),
        Z = as.numeric(Z)
      ) |>
      dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
      dplyr::arrange(time)

    attr(raw$time, "tzone") <- compute_tz
    raw
  }

  add_report("ActiGraph loader started.")
  add_report(sprintf("Autocalibration requested by user: %s.", if (isTRUE(autocalibrate)) "YES" else "NO"))
  add_report(sprintf("Requested agread parser order: %s", paste(parser_fallback, collapse = " -> ")))

  used_calibration <- FALSE
  calibration_note <- "user_disabled"
  calibration_source <- "agcounts_agread"
  cal_source_used <- "native_read_gt3x_no_cal"
  model <- "actigraph_gt3x"

  raw <- NULL

  if (isTRUE(autocalibrate)) {
    agread_fail_msgs <- character()

    for (pr in parser_fallback) {
      add_report(sprintf("agread calibration attempt: parser=%s", pr))
      vmsg("[ACTIGRAPH] agread parser attempt: ", pr)

      res <- try_agread(file_path, parser = pr, compute_tz = tz)

      if (is.null(res$data)) {
        msg <- if (!is.null(res$err)) conditionMessage(res$err) else "unknown agread error"
        agread_fail_msgs <- c(agread_fail_msgs, paste0(pr, ": ", msg))
        add_report(sprintf("agread attempt failed: parser=%s ; reason: %s", pr, msg))
        next
      }

      dat <- tryCatch(
        normalize_agread_output(res$data, compute_tz = tz),
        error = function(e) e
      )

      if (inherits(dat, "error")) {
        msg <- conditionMessage(dat)
        agread_fail_msgs <- c(agread_fail_msgs, paste0(pr, ": ", msg))
        add_report(sprintf("agread attempt failed after read: parser=%s ; reason: %s", pr, msg))
        next
      }

      if (!nrow(dat)) {
        agread_fail_msgs <- c(agread_fail_msgs, paste0(pr, ": empty_data"))
        add_report(sprintf("agread attempt failed: parser=%s ; reason: empty_data", pr))
        next
      }

      raw <- dat
      used_calibration <- identical(pr, "GGIR")
      calibration_note <- if (used_calibration) "ok" else "read_ok_no_calibration"
      cal_source_used <- paste0("agread_", pr)
      calibration_source <- paste0("agcounts_agread_", pr)

      add_report(sprintf("agread attempt succeeded: parser=%s", pr))
      break
    }

    if (is.null(raw)) {
      calibration_note <- paste0(
        "failed_all_agread_attempts:",
        paste(unique(agread_fail_msgs), collapse = " | ")
      )
      add_report("All agread parser attempts failed. Falling back to native read.gt3x path without calibration.")
    }
  }

  if (is.null(raw)) {
    raw <- read_native_gt3x(file_path, compute_tz = tz)

    if (!nrow(raw)) {
      stop("No samples after ActiGraph read: ", basename(file_path))
    }

    cal_source_used <- "native_read_gt3x_fallback"
    if (!isTRUE(autocalibrate)) {
      calibration_note <- "user_disabled"
    }

    add_report("ActiGraph loader used native GT3X read path.")
  } else {
    add_report(sprintf("ActiGraph loader used agread-derived path: %s", cal_source_used))
  }

  if (!nrow(raw)) {
    stop("No samples after ActiGraph read: ", basename(file_path))
  }

  add_report(sprintf("Rows kept after read/filter: %d", nrow(raw)))
  add_report(sprintf(
    "Raw timeline span: first=%s ; last=%s",
    format(raw$time[1], tz = tz, usetz = TRUE),
    format(raw$time[nrow(raw)], tz = tz, usetz = TRUE)
  ))

  raw_summary <- summarize_stream(raw, "raw_read", target_hz = sample_rate, tz_use = tz)
  add_report(report_stream_summary(raw_summary))

  if (!is_regular_strict(raw$time, hz = sample_rate)) {
    add_report(sprintf(
      "Input timeline was not strict at %g Hz. Rebuilding strict grid by sample order.",
      sample_rate
    ))
    raw <- reindex_strict_by_order(raw, hz = sample_rate, compute_tz = tz)
  } else {
    add_report(sprintf("Input timeline already matched strict %g Hz.", sample_rate))
  }

  attr(raw$time, "tzone") <- tz

  pre_final_summary <- summarize_stream(raw, "pre_final_strict", target_hz = sample_rate, tz_use = tz)
  add_report(report_stream_summary(pre_final_summary))

  canon <- canonicalize_final_blocks(
    raw,
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
    stop("No samples after final ActiGraph canonicalization: ", basename(file_path))
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

  spec <- list(
    model = model,
    source_tz_detected = TRUE,
    source_tz = tz,
    compute_tz = tz,
    output_tz = tz,
    tz_rule = "compute_in_source_tz",
    grid_action = "reindexed",
    regularized_for_uniformity = TRUE,
    calibration_input_provenance = "device_loader_native_or_agread_strict_grid",
    calibration_expected = TRUE,
    calibration_source = calibration_source,
    calibration_attempted = isTRUE(autocalibrate),
    calibration_success = used_calibration,
    calibration_note = calibration_note,
    units_choice = "g",
    autocalibrated = used_calibration,
    autocalibrate_note = calibration_note,
    calibration_stream_used = cal_source_used,
    final_canonicalization_applied = canon$n_drop_tail > 0L,
    final_canonicalization_head_dropped = 0L,
    final_canonicalization_tail_dropped = canon$n_drop_tail,
    final_wallclock_rephased = isTRUE(abs(canon$phase_shift_sec) > 0),
    final_wallclock_phase_shift_sec = canon$phase_shift_sec
  )

  attr(out, "ua_time_spec") <- spec
  attr(out, "ua_time_report") <- unique(.time_report)

  out
}
