# R/axivity_loader.R
#' Read Axivity data from either:
#' - OMGUI resampled CSV (already calibrated upstream), or
#' - raw CWA (uncalibrated; optional autocalibration in UA)
#'
#' Design:
#' - .csv path is ONLY for clean OMGUI resampled/calibrated Axivity CSV
#' - .cwa path is for native raw Axivity files via GGIRread::readAxivity()
#' - messy/raw CSV variants should be routed by the driver to the generic loader
#' - no interpolation of X/Y/Z
#' - output is strict monotonic / uniform and whole-second aligned
#'
#' Attached attributes:
#' - attr(out, "ua_time_spec")
#' - attr(out, "ua_time_report")
#'
#' @noRd
read_and_calibrate_axivity <- function(file_path,
                                       sample_rate = 100,
                                       tz = "UTC",
                                       autocalibrate = TRUE,
                                       zero_triplet_eps = 0,
                                       trim_edge_seconds = 60,
                                       verbose = FALSE) {

  if (!is.logical(autocalibrate) || length(autocalibrate) != 1 || is.na(autocalibrate)) {
    stop("`autocalibrate` must be TRUE or FALSE.")
  }

  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")
  if (!requireNamespace("tibble", quietly = TRUE)) stop("Package 'tibble' is required.")

  stopifnot(is.finite(sample_rate), sample_rate > 0)
  stopifnot(is.character(tz), length(tz) == 1, nzchar(tz))
  stopifnot(is.finite(trim_edge_seconds), trim_edge_seconds >= 0)

  file_path <- normalizePath(file_path, winslash = "/", mustWork = TRUE)
  ext <- tolower(tools::file_ext(file_path))

  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

  .time_report <- character()
  add_report <- function(...) {
    .time_report <<- c(.time_report, paste0(...))
    invisible(NULL)
  }

  vmsg <- function(...) {
    if (isTRUE(verbose)) message(...)
  }

  normalize_names <- function(nm) {
    nm <- gsub("\u00A0", " ", nm, perl = TRUE)
    nm <- trimws(nm)
    nm <- tolower(nm)
    nm <- gsub("[^a-z0-9]+", "_", nm)
    nm <- gsub("^_+|_+$", "", nm)
    nm
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
      stop("Axivity final canonicalization failed: invalid or too-short time vector.")
    }

    d <- diff(x)
    d <- d[is.finite(d)]
    if (!length(d)) {
      stop("Axivity final canonicalization failed: could not compute time differences.")
    }

    if (sum(duplicated(tt)) > 0) {
      stop("Axivity final canonicalization failed: duplicated timestamps remained.")
    }
    if (sum(d <= 0) > 0) {
      stop("Axivity final canonicalization failed: nonpositive time differences remained.")
    }

    med_dt <- stats::median(d)
    tol <- max(1e-6, 0.05 * dt)
    if (!is.finite(med_dt) || abs(med_dt - dt) > tol) {
      stop(sprintf(
        "Axivity final canonicalization failed: median_dt=%.9f did not match expected dt=%.9f.",
        med_dt, dt
      ))
    }

    n_keep <- floor(n0 / hz) * hz
    n_drop_tail <- n0 - n_keep
    if (!is.finite(n_keep) || n_keep <= 0) {
      stop("Axivity final canonicalization left fewer than one complete second.")
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

  .as_plain_xyz <- function(df, compute_tz) {
    df <- as.data.frame(df)
    df <- df[, intersect(names(df), c("time", "X", "Y", "Z")), drop = FALSE]

    flatten_num <- function(x) {
      if (is.list(x)) as.numeric(unlist(x, use.names = FALSE)) else as.numeric(x)
    }

    df$time <- as.POSIXct(df$time, tz = compute_tz)
    attr(df$time, "tzone") <- compute_tz
    df$X <- flatten_num(df$X)
    df$Y <- flatten_num(df$Y)
    df$Z <- flatten_num(df$Z)

    ok <- !is.na(df$time) & is.finite(df$X) & is.finite(df$Y) & is.finite(df$Z)
    df <- df[ok, , drop = FALSE]
    df[order(df$time), , drop = FALSE]
  }

  .drop_zero_triplets <- function(df, eps = 0) {
    keep <- !((abs(df$X) <= eps) & (abs(df$Y) <= eps) & (abs(df$Z) <= eps))
    list(df = df[keep, , drop = FALSE], n_dropped = sum(!keep))
  }

  trim_interior <- function(df, hz, edge_seconds = 60) {
    n <- nrow(df)
    if (!is.finite(edge_seconds) || edge_seconds <= 0 || n <= 0) return(df)

    k <- as.integer(round(edge_seconds * hz))
    if (!is.finite(k) || k <= 0) return(df)

    if (n <= (2L * k + hz)) {
      return(df)
    }

    df[(k + 1L):(n - k), , drop = FALSE]
  }

  try_agcalibrate <- function(df, compute_tz) {
    err <- NULL
    df_plain <- .as_plain_xyz(df, compute_tz = compute_tz)

    cal <- tryCatch(
      agcounts::agcalibrate(raw = df_plain, verbose = FALSE, tz = compute_tz),
      error = function(e) {
        err <<- e
        NULL
      }
    )
    list(cal = cal, err = err)
  }

  validate_cal_output <- function(input_df, cal_df, compute_tz, hz) {
    if (!all(c("time", "X", "Y", "Z") %in% names(cal_df))) {
      return(list(ok = FALSE, reason = "missing_required_columns"))
    }

    cal_df <- as.data.frame(cal_df)
    cal_df$time <- as.POSIXct(cal_df$time, tz = compute_tz)
    attr(cal_df$time, "tzone") <- compute_tz

    if (nrow(cal_df) != nrow(input_df)) {
      return(list(ok = FALSE, reason = sprintf("row_count_mismatch:%d->%d", nrow(input_df), nrow(cal_df))))
    }
    if (anyNA(cal_df$time)) {
      return(list(ok = FALSE, reason = "na_time_after_cal"))
    }
    if (sum(duplicated(cal_df$time)) > 0) {
      return(list(ok = FALSE, reason = "duplicated_time_after_cal"))
    }

    d <- diff(as.numeric(cal_df$time))
    d <- d[is.finite(d)]
    if (length(d)) {
      if (sum(d <= 0) > 0) {
        return(list(ok = FALSE, reason = "nonpositive_dt_after_cal"))
      }
      med_dt <- stats::median(d)
      target_dt <- 1 / hz
      dt_tol <- max(0.002, 0.25 * target_dt)
      if (!is.finite(med_dt) || abs(med_dt - target_dt) > dt_tol) {
        return(list(ok = FALSE, reason = sprintf("median_dt_off:%.9f_vs_%.9f", med_dt, target_dt)))
      }
    }

    list(ok = TRUE, reason = "ok")
  }

  is_omgui_resampled_axivity_csv <- function(path) {
    hdr <- tryCatch(
      utils::read.csv(path, nrows = 5, stringsAsFactors = FALSE, check.names = FALSE),
      error = function(e) NULL
    )
    if (is.null(hdr) || !ncol(hdr)) return(FALSE)

    nm <- normalize_names(names(hdr))

    has_time <- any(nm %in% c("time", "timestamp", "date_time", "datetime"))
    has_x <- any(nm %in% c("accel_x_g", "accel_x", "x"))
    has_y <- any(nm %in% c("accel_y_g", "accel_y", "y"))
    has_z <- any(nm %in% c("accel_z_g", "accel_z", "z"))

    isTRUE(has_time && has_x && has_y && has_z)
  }

  read_resampled_csv_native <- function(path, tz_use) {
    df0 <- utils::read.csv(path, header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
    names(df0) <- normalize_names(names(df0))

    time_candidates <- c("time", "timestamp", "date_time", "datetime")
    x_candidates <- c("accel_x_g", "accel_x", "x")
    y_candidates <- c("accel_y_g", "accel_y", "y")
    z_candidates <- c("accel_z_g", "accel_z", "z")

    tc <- intersect(time_candidates, names(df0))[1]
    xc <- intersect(x_candidates, names(df0))[1]
    yc <- intersect(y_candidates, names(df0))[1]
    zc <- intersect(z_candidates, names(df0))[1]

    if (any(is.na(c(tc, xc, yc, zc)))) {
      stop(
        "AXIVITY_NON_OMGUI_CSV: Axivity CSV did not match the expected OMGUI resampled format. ",
        "Dispatcher should reroute this file to the generic loader."
      )
    }

    df <- tibble::tibble(
      time = as.POSIXct(df0[[tc]], format = "%Y-%m-%d %H:%M:%OS", tz = tz_use),
      X = as.numeric(df0[[xc]]),
      Y = as.numeric(df0[[yc]]),
      Z = as.numeric(df0[[zc]])
    ) |>
      dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
      dplyr::arrange(time)

    attr(df$time, "tzone") <- tz_use
    df
  }

  read_cwa_native <- function(path, tz_use) {
    if (!requireNamespace("GGIRread", quietly = TRUE)) {
      stop("Package 'GGIRread' is required to read Axivity .cwa files.")
    }

    hdr_obj <- tryCatch(
      GGIRread::readAxivity(
        filename = path,
        start = 0,
        end = 0,
        progressBar = FALSE
      ),
      error = function(e) {
        stop("GGIRread header read failed for file '", basename(path),
             "'. Reason: ", conditionMessage(e))
      }
    )

    hdr <- hdr_obj$header
    if (is.null(hdr) || is.null(hdr$blocks) || !is.finite(hdr$blocks) || hdr$blocks <= 0) {
      stop("GGIRread header did not provide a valid block count for file '",
           basename(path), "'.")
    }

    add_report(sprintf("GGIRread header block count: %d", as.integer(hdr$blocks)))

    obj <- tryCatch(
      GGIRread::readAxivity(
        filename = path,
        start = 0,
        end = as.integer(hdr$blocks),
        header = hdr,
        progressBar = FALSE
      ),
      error = function(e) {
        stop("GGIRread full read failed for file '", basename(path),
             "'. Reason: ", conditionMessage(e))
      }
    )

    dat <- obj$data
    if (is.null(dat) || !is.data.frame(dat) || !nrow(dat)) {
      stop("GGIRread returned no usable data rows for file '", basename(path), "'.")
    }

    add_report(sprintf("GGIRread rows returned: %d", nrow(dat)))

    names(dat) <- normalize_names(names(dat))

    time_candidates <- c("time", "timestamp", "datetime", "date_time")
    x_candidates <- c("x", "accel_x", "ax", "x_g")
    y_candidates <- c("y", "accel_y", "ay", "y_g")
    z_candidates <- c("z", "accel_z", "az", "z_g")

    tc <- intersect(time_candidates, names(dat))[1]
    xc <- intersect(x_candidates, names(dat))[1]
    yc <- intersect(y_candidates, names(dat))[1]
    zc <- intersect(z_candidates, names(dat))[1]

    if (any(is.na(c(tc, xc, yc, zc)))) {
      stop(
        "Could not detect time/X/Y/Z columns in GGIRread Axivity output for file '",
        basename(path), "'. Found columns: ", paste(names(dat), collapse = ", ")
      )
    }

    tt_raw <- dat[[tc]]

    if (inherits(tt_raw, "POSIXt")) {
      tt <- as.POSIXct(tt_raw, tz = tz_use)
    } else if (is.numeric(tt_raw) || is.integer(tt_raw)) {
      tt <- as.POSIXct(as.numeric(tt_raw), origin = "1970-01-01", tz = tz_use)
    } else {
      tt <- as.POSIXct(tt_raw, tz = tz_use)
    }

    df <- tibble::tibble(
      time = tt,
      X = as.numeric(dat[[xc]]),
      Y = as.numeric(dat[[yc]]),
      Z = as.numeric(dat[[zc]])
    ) |>
      dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
      dplyr::arrange(time)

    attr(df$time, "tzone") <- tz_use

    if (!nrow(df)) {
      stop("No usable rows remained after filtering GGIRread Axivity output for file '",
           basename(path), "'.")
    }

    df
  }

  add_report(paste0("Axivity input file: ", basename(file_path)))
  add_report(sprintf("Autocalibration requested by user: %s.", if (isTRUE(autocalibrate)) "YES" else "NO"))

  calibration_attempted <- FALSE
  calibration_success <- FALSE
  calibration_note <- "not_attempted"
  calibration_source <- NA_character_
  calibration_input_provenance <- NA_character_
  grid_action <- "reindexed"
  regularized_for_uniformity <- TRUE
  model <- NA_character_
  read_source <- basename(file_path)
  raw <- NULL

  if (ext == "csv") {
    add_report("Axivity CSV branch selected.")

    if (!is_omgui_resampled_axivity_csv(file_path)) {
      stop(
        "AXIVITY_NON_OMGUI_CSV: Axivity CSV did not match the expected OMGUI resampled format. ",
        "Dispatcher should reroute this file to the generic loader."
      )
    }

    add_report("Axivity CSV matched OMGUI resampled format.")
    add_report("Axivity loader expects this CSV to be calibrated upstream.")

    raw <- read_resampled_csv_native(file_path, tz_use = tz)

    if (!nrow(raw)) stop("No samples after Axivity CSV read: ", basename(file_path))

    calibration_attempted <- FALSE
    calibration_success <- TRUE
    calibration_note <- "already_calibrated_upstream"
    calibration_source <- "upstream_resampled_calibrated"
    calibration_input_provenance <- "upstream_calibrated_resampled_csv"
    model <- "axivity_resampled_csv"
    read_source <- "resampled_csv"

    add_report("UA did not perform additional calibration in the Axivity CSV loader.")
  } else if (ext == "cwa") {
    add_report("Axivity CWA branch selected.")
    add_report("Axivity CWA is treated as raw/uncalibrated input.")
    add_report("CWA read path uses GGIRread::readAxivity().")

    raw <- read_cwa_native(file_path, tz_use = tz)

    if (!nrow(raw)) stop("No samples after Axivity CWA read: ", basename(file_path))

    model <- "axivity_cwa"
    calibration_source <- "agcounts_agcalibrate"
    calibration_input_provenance <- "native_cwa_raw"

    raw_pre_summary <- summarize_stream(raw, "axivity_cwa_raw_read", target_hz = sample_rate, tz_use = tz)
    add_report(report_stream_summary(raw_pre_summary))

    if (!is_regular_strict(raw$time, hz = sample_rate)) {
      add_report(sprintf(
        "Input timeline was not strict at %g Hz. Rebuilding strict grid by sample order before calibration.",
        sample_rate
      ))
      raw <- reindex_strict_by_order(raw, hz = sample_rate, compute_tz = tz)
    } else {
      add_report(sprintf("Input timeline already matched strict %g Hz.", sample_rate))
    }

    vm <- sqrt(raw$X^2 + raw$Y^2 + raw$Z^2)
    vm <- vm[is.finite(vm)]
    vm_iqr <- if (length(vm)) stats::IQR(vm, na.rm = TRUE) else NA_real_
    vm_sd  <- if (length(vm)) stats::sd(vm, na.rm = TRUE) else NA_real_

    calibration_attempted <- FALSE
    calibration_success <- FALSE
    calibration_note <- if (isTRUE(autocalibrate)) "not_attempted" else "user_disabled"

    if (isTRUE(autocalibrate)) {
      if (!requireNamespace("agcounts", quietly = TRUE)) {
        stop("Package 'agcounts' is required when autocalibrate = TRUE for Axivity .cwa files.")
      }

      if ((!is.na(vm_iqr) && vm_iqr <= 1e-6) || (!is.na(vm_sd) && vm_sd <= 1e-6)) {
        calibration_attempted <- FALSE
        calibration_success <- FALSE
        calibration_note <- "skipped_degenerate_signal_variation"
        add_report("Autocalibration skipped because signal variation was degenerate; proceeding with uncalibrated CWA stream.")
      } else {
        calibration_attempted <- TRUE

        attempts <- list()
        attempts[["raw_strict"]] <- raw

        z1 <- .drop_zero_triplets(raw, eps = zero_triplet_eps)
        if (z1$n_dropped > 0) {
          attempts[["raw_strict_zero_dropped"]] <- z1$df
        }

        t1 <- trim_interior(raw, hz = sample_rate, edge_seconds = trim_edge_seconds)
        if (nrow(t1) < nrow(raw)) {
          attempts[["trimmed_interior"]] <- t1
        }

        z2 <- .drop_zero_triplets(t1, eps = zero_triplet_eps)
        if (nrow(t1) < nrow(raw) && z2$n_dropped > 0) {
          attempts[["trimmed_interior_zero_dropped"]] <- z2$df
        }

        fail_msgs <- character()
        cal_ok <- FALSE
        cal_used <- raw
        cal_stream_used <- "cwa_raw_fallback"

        for (nm in names(attempts)) {
          dat <- attempts[[nm]]
          if (!nrow(dat)) next

          vm_a <- sqrt(dat$X^2 + dat$Y^2 + dat$Z^2)
          vm_a <- vm_a[is.finite(vm_a)]
          if (length(vm_a) < max(1000, sample_rate * 60)) {
            fail_msgs <- c(fail_msgs, paste0(nm, ": insufficient_rows_for_calibration"))
            add_report(sprintf("Calibration attempt skipped: %s ; reason: insufficient_rows_for_calibration", nm))
            next
          }

          vm_a_iqr <- stats::IQR(vm_a, na.rm = TRUE)
          vm_a_sd  <- stats::sd(vm_a, na.rm = TRUE)

          if ((!is.na(vm_a_iqr) && vm_a_iqr <= 1e-6) || (!is.na(vm_a_sd) && vm_a_sd <= 1e-6)) {
            fail_msgs <- c(fail_msgs, paste0(nm, ": degenerate_signal_variation"))
            add_report(sprintf("Calibration attempt skipped: %s ; reason: degenerate_signal_variation", nm))
            next
          }

          add_report(sprintf("Calibration attempt: %s ; n=%d", nm, nrow(dat)))
          vmsg("[AXIVITY] calibration attempt: ", nm)

          res <- try_agcalibrate(dat, compute_tz = tz)

          if (is.null(res$cal)) {
            msg <- if (!is.null(res$err)) conditionMessage(res$err) else "unknown calibration error"
            fail_msgs <- c(fail_msgs, paste0(nm, ": ", msg))
            add_report(sprintf("Calibration attempt failed: %s ; reason: %s", nm, msg))
            next
          }

          cal_tmp <- as.data.frame(res$cal)
          val <- validate_cal_output(.as_plain_xyz(dat, compute_tz = tz), cal_tmp, compute_tz = tz, hz = sample_rate)

          if (!isTRUE(val$ok)) {
            fail_msgs <- c(fail_msgs, paste0(nm, ": rejected(", val$reason, ")"))
            add_report(sprintf("Calibration attempt rejected: %s ; reason: %s", nm, val$reason))
            next
          }

          add_report(sprintf("Calibration attempt succeeded: %s", nm))

          if (nrow(dat) == nrow(raw)) {
            cal_used <- .as_plain_xyz(
              tibble::tibble(
                time = as.POSIXct(cal_tmp$time, tz = tz),
                X = as.numeric(cal_tmp$X),
                Y = as.numeric(cal_tmp$Y),
                Z = as.numeric(cal_tmp$Z)
              ),
              compute_tz = tz
            )

            cal_stream_used <- nm
            calibration_success <- TRUE
            calibration_note <- "ok"
            cal_ok <- TRUE
            break
          } else {
            fail_msgs <- c(fail_msgs, paste0(nm, ": subset_calibration_only"))
            add_report(sprintf(
              paste0(
                "Calibration succeeded on reduced subset '%s' but row count differs from full file. ",
                "UA retains the full uncalibrated stream rather than returning partial calibrated data."
              ),
              nm
            ))
          }
        }

        if (!isTRUE(cal_ok)) {
          read_source <- "cwa_raw_fallback"
          calibration_success <- FALSE
          calibration_note <- paste0(
            "failed_all_attempts:",
            paste(unique(fail_msgs), collapse = " | ")
          )
          add_report("All calibration attempts failed or were rejected. Proceeding with uncalibrated Axivity CWA stream.")
        } else {
          raw <- cal_used
          read_source <- cal_stream_used
        }
      }
    } else {
      add_report("Autocalibration disabled by user. Proceeding with strict uncalibrated Axivity CWA stream.")
      read_source <- "cwa_raw_no_cal"
    }
  } else {
    stop(
      "Unsupported Axivity file type: .", ext, "\n",
      "This loader supports:\n",
      "- .cwa   (raw native Axivity)\n",
      "- .csv   (clean OMGUI resampled/calibrated CSV only)\n",
      "For other raw/messy Axivity CSV formats, the dispatcher should route to the generic loader."
    )
  }

  raw_summary <- summarize_stream(raw, "axivity_input_used", target_hz = sample_rate, tz_use = tz)
  add_report(report_stream_summary(raw_summary))

  if (!is_regular_strict(raw$time, hz = sample_rate)) {
    add_report(sprintf(
      "Timeline entering final canonicalization was not strict at %g Hz. Rebuilding strict grid by sample order.",
      sample_rate
    ))
    raw <- reindex_strict_by_order(raw, hz = sample_rate, compute_tz = tz)
  }

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
    stop("No samples after final Axivity canonicalization: ", basename(file_path))
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

  add_report("No X/Y/Z interpolation was performed in UA.")

  final_summary <- summarize_stream(out, "final_output", target_hz = sample_rate, tz_use = tz)
  add_report(report_stream_summary(final_summary))

  spec <- list(
    model = model,
    source_tz_detected = TRUE,
    source_tz = tz,
    compute_tz = tz,
    output_tz = tz,
    tz_rule = "compute_in_source_tz",
    grid_action = grid_action,
    regularized_for_uniformity = regularized_for_uniformity,
    calibration_input_provenance = calibration_input_provenance,
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
  attr(out, "ua_time_report") <- unique(.time_report)

  out
}
