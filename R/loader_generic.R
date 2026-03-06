# R/loader_generic.R
#' Generic loader for regularly sampled accelerometer tables
#'
#' Design goals:
#' - robust to headered or headerless files
#' - robust to metadata blocks before the real numeric table
#' - robust to odd axis names and odd time encodings
#' - strict about selecting a credible absolute timeline
#' - transparent about timezone handling via attached attributes
#'
#' Timezone policy:
#' 1) If timezone/offset is explicitly encoded in raw strings, use that information.
#' 2) If time is numeric epoch-based, treat it as a UTC-origin instant.
#' 3) If time is naive local clock text, interpret it in the chosen source tz:
#'      - requested `tz` if provided
#'      - otherwise "UTC"
#' 4) Compute-time uses the source timeline, not the display/output timezone.
#'
#' Important:
#' - NO interpolation of X/Y/Z is performed.
#' - If timestamps are irregular, UA may:
#'   * snap to nearest grid tick and collapse duplicates by mean, or
#'   * rebuild a strict time index preserving sample order.
#'   Neither step interpolates missing values.
#'
#' Attached attributes for driver logging:
#' - attr(out, "ua_time_spec")
#' - attr(out, "ua_time_report")
#'
#' @noRd
read_and_calibrate_generic <- function(file_path,
                                       sample_rate = 100,
                                       tz = "UTC",
                                       autocalibrate = c("auto", "true", "false"),
                                       units = c("auto", "g", "m/s2"),
                                       unit_guard_min_n = 5000,
                                       unit_ratio_cutoff = 0.35,
                                       cal_vm_dev_threshold = 0.03,
                                       zero_triplet_eps = 0,
                                       time_col = NULL,
                                       x_col = NULL,
                                       y_col = NULL,
                                       z_col = NULL,
                                       delim = NULL,
                                       header = NA,
                                       max_scan = 200000,
                                       verbose = FALSE) {

  autocalibrate <- match.arg(tolower(autocalibrate), choices = c("auto", "true", "false"))
  units <- match.arg(units)

  stopifnot(is.finite(sample_rate), sample_rate > 0)
  stopifnot(is.character(tz), length(tz) == 1, nzchar(tz))

  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Package 'stringr' is required.")
  if (!requireNamespace("tibble", quietly = TRUE)) stop("Package 'tibble' is required.")

  file_path <- normalizePath(file_path, winslash = "/", mustWork = TRUE)
  ext <- tolower(tools::file_ext(file_path))

  # ---------------------------------------------------------------------------
  # logging helpers
  # ---------------------------------------------------------------------------
  .time_report <- character()

  add_report <- function(...) {
    .time_report <<- c(.time_report, paste0(...))
    invisible(NULL)
  }

  vmsg <- function(...) {
    if (isTRUE(verbose)) message(...)
  }

  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

  # ---------------------------------------------------------------------------
  # small utilities
  # ---------------------------------------------------------------------------
  normalize_names <- function(nm) {
    nm <- gsub("\u00A0", " ", nm, perl = TRUE)
    nm <- trimws(nm)
    nm <- tolower(nm)
    nm <- gsub("[^a-z0-9]+", "_", nm)
    nm <- gsub("^_+|_+$", "", nm)
    nm
  }

  resolve_col <- function(df, col) {
    if (is.null(col)) return(NULL)
    if (is.numeric(col) || is.integer(col)) {
      j <- as.integer(col)
      if (!is.finite(j) || j < 1L || j > ncol(df)) stop("Column index out of range: ", col)
      return(names(df)[j])
    }
    col <- as.character(col)
    if (!(col %in% names(df))) {
      stop("Column not found: ", col, "\nAvailable: ", paste(names(df), collapse = ", "))
    }
    col
  }

  year_penalty <- function(tt) {
    yy <- suppressWarnings(as.integer(format(tt[1], "%Y")))
    if (!is.finite(yy)) return(1e6)
    if (yy < 1990 || yy > 2100) return(1000)
    0
  }

  score_regular <- function(tt_num, target_dt) {
    tt_num <- tt_num[is.finite(tt_num)]
    if (length(tt_num) < 50) return(Inf)
    d <- diff(tt_num)
    d <- d[is.finite(d) & d > 0]
    if (!length(d)) return(Inf)
    med <- stats::median(d)
    mad <- stats::median(abs(d - med))
    abs(med - target_dt) + mad
  }

  is_regular_enough <- function(tt, hz, tol = NULL, max_bad_frac = 0.01) {
    dt <- 1 / hz
    if (is.null(tol)) tol <- max(0.002, 0.20 * dt)
    x <- as.numeric(tt)
    d <- diff(x)
    d <- d[is.finite(d) & d > 0]
    if (!length(d)) return(FALSE)
    bad <- mean(abs(d - dt) > tol)
    med_ok <- abs(stats::median(d) - dt) <= tol
    isTRUE(med_ok && bad <= max_bad_frac)
  }

  # ---------------------------------------------------------------------------
  # detect metadata block / real data start
  # ---------------------------------------------------------------------------
  detect_data_start_row <- function(path, n_lines = 2000, min_cols = 4, win = 10) {
    if (!(ext %in% c("csv", "gz"))) return(0L)

    lines <- readLines(path, n = n_lines, warn = FALSE)
    if (!length(lines)) return(0L)

    delim_use <- delim
    if (is.null(delim_use)) {
      sniff <- paste(lines[1:min(length(lines), 10)], collapse = "\n")
      c_comma <- stringr::str_count(sniff, ",")
      c_semi  <- stringr::str_count(sniff, ";")
      c_tab   <- stringr::str_count(sniff, "\t")
      delim_use <- names(which.max(c("," = c_comma, ";" = c_semi, "\t" = c_tab)))
    }

    split_line <- function(s) strsplit(s, split = delim_use, fixed = TRUE)[[1]]
    is_num_tok <- function(x) {
      x <- gsub("\"", "", x)
      x <- trimws(x)
      grepl("^[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?$", x)
    }

    n_tok <- integer(length(lines))
    p_num <- numeric(length(lines))

    for (i in seq_along(lines)) {
      tok <- trimws(split_line(lines[i]))
      tok <- tok[nzchar(tok)]
      n_tok[i] <- length(tok)
      p_num[i] <- if (length(tok)) mean(is_num_tok(tok)) else 0
    }

    for (i in seq_along(lines)) {
      if (n_tok[i] < min_cols) next
      if (p_num[i] < 0.70) next
      j2 <- min(length(lines), i + win - 1L)
      if (j2 <= i) break
      if (median(n_tok[i:j2]) < min_cols) next
      if (mean(p_num[i:j2] >= 0.70) < 0.80) next
      return(as.integer(i - 1L))
    }

    0L
  }

  # ---------------------------------------------------------------------------
  # timestamp string cleanup / parsing
  # ---------------------------------------------------------------------------
  canon_ts_chr <- function(x) {
    s <- stringr::str_trim(as.character(x))
    s <- gsub("\u00A0", " ", s, perl = TRUE)
    s <- sub("^[\ufeff\uFEFF]+", "", s, perl = TRUE)
    s <- gsub(",", ".", s, fixed = TRUE)

    s <- ifelse(stringr::str_detect(s, "^\\d{4}-\\d{2}-\\d{2}$"),
                paste0(s, " 00:00:00.000"), s)

    s <- ifelse(stringr::str_detect(s, "^\\d{4}-\\d{2}-\\d{2}\\.\\d{1,6}$"),
                paste0(stringr::str_sub(s, 1, 10), " 00:00:00", stringr::str_sub(s, 11)),
                s)

    s <- ifelse(stringr::str_detect(s, "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}$"),
                paste0(s, ":00.000"), s)

    s <- sub("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2})\\.([0-9]{1,6})$",
             "\\1:00.\\2", s, perl = TRUE)

    s <- ifelse(stringr::str_detect(s, "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),
                paste0(s, ".000"), s)

    s <- ifelse(stringr::str_detect(s, "^\\d{1,2}:\\d{2}$"),
                paste0(s, ":00.000"), s)

    s <- sub("^([0-9]{1,2}:[0-9]{2})\\.([0-9]{1,6})$",
             "\\1:00.\\2", s, perl = TRUE)

    s <- ifelse(stringr::str_detect(s, "^\\d{1,2}:\\d{2}:\\d{2}$"),
                paste0(s, ".000"), s)

    s <- sub("^(.*\\.[0-9]{3})[0-9]+$", "\\1", s, perl = TRUE)
    s <- sub("^(.*\\.[0-9]{2})$", "\\10", s, perl = TRUE)
    s <- sub("^(.*\\.[0-9]{1})$", "\\100", s, perl = TRUE)

    s
  }

  detect_explicit_tz <- function(x_chr) {
    s <- canon_ts_chr(x_chr)
    has_z <- stringr::str_detect(s, "Z$")
    has_off <- stringr::str_detect(s, "[+-]\\d{2}:?\\d{2}$")
    any(has_z | has_off, na.rm = TRUE)
  }

  parse_time_string_detectable_tz <- function(x_chr) {
    s <- canon_ts_chr(x_chr)
    suppressWarnings(lubridate::parse_date_time(
      s,
      orders = c(
        "Ymd HMSOSz", "Y-m-d H:M:S!OSz",
        "Ymd HMSz",   "Y-m-d H:M:Sz",
        "mdY HMSOSz", "m/d/Y H:M:S!OSz", "m/d/y H:M:S!OSz",
        "mdY HMSz",   "m/d/Y H:M:Sz",    "m/d/y H:M:Sz"
      ),
      tz = "UTC",
      exact = FALSE
    ))
  }

  parse_time_string_local <- function(x_chr, tz_local) {
    s <- canon_ts_chr(x_chr)
    s2 <- sub("(Z|[+-]\\d{2}:?\\d{2})$", "", s, perl = TRUE)

    suppressWarnings(lubridate::parse_date_time(
      s2,
      orders = c(
        "Ymd HMSOS", "Y-m-d H:M:S!OS",
        "Ymd HMS",   "Y-m-d H:M:S",
        "Ymd HMOS",  "Y-m-d H:M!OS",
        "Ymd HM",    "Y-m-d H:M",
        "mdY HMSOS", "m/d/Y H:M:S!OS", "m/d/y H:M:S!OS",
        "mdY HMS",   "m/d/Y H:M:S",    "m/d/y H:M:S",
        "mdY HM",    "m/d/Y H:M",      "m/d/y H:M",
        "Ymd", "Y-m-d"
      ),
      tz = tz_local,
      exact = FALSE
    ))
  }

  parse_time_of_day_only <- function(x_chr, tz_local) {
    s <- canon_ts_chr(x_chr)
    is_tod <- stringr::str_detect(s, "^\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}$")
    if (!any(is_tod)) return(NULL)

    date_hit <- stringr::str_extract(s, "\\d{4}-\\d{2}-\\d{2}")
    anchor <- date_hit[which(!is.na(date_hit))[1]]
    if (is.na(anchor)) return(NULL)

    tod <- s[is_tod]
    out <- as.POSIXct(paste(anchor, tod), tz = tz_local, format = "%Y-%m-%d %H:%M:%OS3")
    full <- rep(as.POSIXct(NA_real_, origin = "1970-01-01", tz = tz_local), length(s))
    full[is_tod] <- out
    full
  }

  parse_time_numeric_best_utc <- function(x_num, target_dt = 1 / sample_rate) {
    x0 <- suppressWarnings(as.numeric(x_num))
    if (all(is.na(x0))) {
      return(list(time_utc = as.POSIXct(rep(NA_real_, length(x_num)), origin = "1970-01-01", tz = "UTC"),
                  model = NA_character_,
                  score = Inf))
    }

    x <- x0[is.finite(x0)]
    if (length(x) < 50) {
      return(list(time_utc = as.POSIXct(rep(NA_real_, length(x_num)), origin = "1970-01-01", tz = "UTC"),
                  model = NA_character_,
                  score = Inf))
    }

    cand <- list(
      epoch_s  = function(v) as.POSIXct(v, origin = "1970-01-01", tz = "UTC"),
      epoch_ms = function(v) as.POSIXct(v / 1e3, origin = "1970-01-01", tz = "UTC"),
      epoch_us = function(v) as.POSIXct(v / 1e6, origin = "1970-01-01", tz = "UTC"),
      epoch_ns = function(v) as.POSIXct(v / 1e9, origin = "1970-01-01", tz = "UTC"),
      excel    = function(v) as.POSIXct((v - 25569) * 86400, origin = "1970-01-01", tz = "UTC")
    )

    scores <- vapply(names(cand), function(nm) {
      tt <- try(cand[[nm]](x), silent = TRUE)
      if (inherits(tt, "try-error")) return(Inf)
      score_regular(as.numeric(tt), target_dt) + year_penalty(tt)
    }, numeric(1))

    best <- names(scores)[which.min(scores)]
    tt_all <- cand[[best]](x0)

    vmsg("[GENERIC] numeric time parse chose: ", best, " (score=", signif(min(scores), 4), ")")

    list(time_utc = tt_all, model = best, score = min(scores))
  }

  detect_epoch_plus_elapsed <- function(df, target_dt = 1 / sample_rate) {
    num_idx <- which(vapply(df, function(x) is.numeric(x) || is.integer(x), logical(1)))
    if (length(num_idx) < 2) return(NULL)

    n <- nrow(df)
    idx <- unique(round(seq(1, n, length.out = min(200000, n))))

    best <- list(score = Inf, anchor = NULL, elapsed = NULL, elapsed_unit = NULL, anchor_model = NULL)

    score_elapsed_scale <- function(deltas, unit_scale) {
      dt_sec <- deltas * unit_scale
      score_regular(c(0, cumsum(dt_sec)), target_dt)
    }

    for (a in num_idx) {
      anchor_parse <- parse_time_numeric_best_utc(df[[a]][idx], target_dt = target_dt)
      if (is.na(anchor_parse$model)) next
      if (!is.finite(anchor_parse$score) || anchor_parse$score > 100) next

      ok_rate <- mean(!is.na(anchor_parse$time_utc))
      if (!is.finite(ok_rate) || ok_rate < 0.90) next

      for (e in setdiff(num_idx, a)) {
        v <- suppressWarnings(as.numeric(df[[e]][idx]))
        if (all(!is.finite(v))) next
        v <- v[is.finite(v)]
        if (length(v) < 200) next

        dv <- diff(v)
        dv <- dv[is.finite(dv)]
        if (!length(dv)) next
        frac_pos <- mean(dv > 0)
        if (!is.finite(frac_pos) || frac_pos < 0.90) next

        dv_pos <- dv[dv > 0]
        if (length(dv_pos) < 50) next

        units_try <- list(ns = 1e-9, us = 1e-6, ms = 1e-3, s = 1)
        for (unm in names(units_try)) {
          sc <- score_elapsed_scale(dv_pos, units_try[[unm]]) + anchor_parse$score
          if (is.finite(sc) && sc < best$score) {
            best <- list(
              score = sc,
              anchor = a,
              elapsed = e,
              elapsed_unit = unm,
              anchor_model = anchor_parse$model
            )
          }
        }
      }
    }

    if (!is.finite(best$score) || is.null(best$anchor) || is.null(best$elapsed)) return(NULL)
    best
  }

  # ---------------------------------------------------------------------------
  # internal compute time policy
  # ---------------------------------------------------------------------------
  as_compute_tz <- function(time_vec, compute_tz) {
    if (identical(compute_tz, "UTC")) {
      out <- as.POSIXct(time_vec, tz = "UTC")
      attr(out, "tzone") <- "UTC"
      return(out)
    }
    out <- as.POSIXct(format(time_vec, tz = compute_tz, usetz = FALSE), tz = compute_tz)
    attr(out, "tzone") <- compute_tz
    out
  }

  parse_time_any <- function(df, col_idx, requested_tz) {
    x <- df[[col_idx]]

    if (inherits(x, "POSIXt")) {
      src_tz <- attr(x, "tzone") %||% "UTC"
      t_comp <- as.POSIXct(x, tz = src_tz)
      attr(t_comp, "tzone") <- src_tz
      return(list(
        time = t_comp,
        spec = list(
          time_source_type = "posix",
          source_tz_detected = TRUE,
          source_tz = src_tz,
          compute_tz = src_tz,
          output_tz = requested_tz,
          tz_rule = "compute_in_source_tz",
          model = "posix",
          score = 0
        )
      ))
    }

    if (is.numeric(x) || is.integer(x)) {
      p <- parse_time_numeric_best_utc(x, target_dt = 1 / sample_rate)
      t_comp <- as.POSIXct(p$time_utc, tz = "UTC")
      attr(t_comp, "tzone") <- "UTC"
      return(list(
        time = t_comp,
        spec = list(
          time_source_type = p$model,
          source_tz_detected = TRUE,
          source_tz = "UTC",
          compute_tz = "UTC",
          output_tz = requested_tz,
          tz_rule = "compute_in_source_tz",
          model = p$model,
          score = p$score
        )
      ))
    }

    x_chr <- as.character(x)

    if (detect_explicit_tz(x_chr)) {
      out <- parse_time_string_detectable_tz(x_chr)
      t_comp <- as.POSIXct(out, tz = "UTC")
      attr(t_comp, "tzone") <- "UTC"
      return(list(
        time = t_comp,
        spec = list(
          time_source_type = "datetime_with_offset",
          source_tz_detected = TRUE,
          source_tz = "encoded_in_raw",
          compute_tz = "UTC",
          output_tz = requested_tz,
          tz_rule = "compute_in_source_tz",
          model = "datetime_with_offset",
          score = score_regular(as.numeric(t_comp), 1 / sample_rate) + year_penalty(t_comp)
        )
      ))
    }

    tod <- parse_time_of_day_only(x_chr, tz_local = requested_tz)
    if (!is.null(tod) && any(!is.na(tod))) {
      t_comp <- as.POSIXct(tod, tz = requested_tz)
      attr(t_comp, "tzone") <- requested_tz
      return(list(
        time = t_comp,
        spec = list(
          time_source_type = "tod_plus_detected_date",
          source_tz_detected = FALSE,
          source_tz = requested_tz,
          compute_tz = requested_tz,
          output_tz = requested_tz,
          tz_rule = "compute_in_assumed_source_tz",
          model = "tod_plus_date",
          score = score_regular(as.numeric(t_comp), 1 / sample_rate) + year_penalty(t_comp)
        )
      ))
    }

    out <- parse_time_string_local(x_chr, tz_local = requested_tz)
    if (!all(is.na(out))) {
      t_comp <- as.POSIXct(out, tz = requested_tz)
      attr(t_comp, "tzone") <- requested_tz
      return(list(
        time = t_comp,
        spec = list(
          time_source_type = "naive_datetime",
          source_tz_detected = FALSE,
          source_tz = requested_tz,
          compute_tz = requested_tz,
          output_tz = requested_tz,
          tz_rule = "compute_in_assumed_source_tz",
          model = "datetime_string_local",
          score = score_regular(as.numeric(t_comp), 1 / sample_rate) + year_penalty(t_comp)
        )
      ))
    }

    num <- suppressWarnings(as.numeric(x_chr))
    if (!all(is.na(num))) {
      p <- parse_time_numeric_best_utc(num, target_dt = 1 / sample_rate)
      t_comp <- as.POSIXct(p$time_utc, tz = "UTC")
      attr(t_comp, "tzone") <- "UTC"
      return(list(
        time = t_comp,
        spec = list(
          time_source_type = p$model,
          source_tz_detected = TRUE,
          source_tz = "UTC",
          compute_tz = "UTC",
          output_tz = requested_tz,
          tz_rule = "compute_in_source_tz",
          model = p$model,
          score = p$score
        )
      ))
    }

    list(
      time = as.POSIXct(rep(NA_real_, nrow(df)), origin = "1970-01-01", tz = "UTC"),
      spec = list(
        time_source_type = NA_character_,
        source_tz_detected = FALSE,
        source_tz = NA_character_,
        compute_tz = "UTC",
        output_tz = requested_tz,
        tz_rule = NA_character_,
        model = NA_character_,
        score = Inf
      )
    )
  }

  # ---------------------------------------------------------------------------
  # X/Y/Z and time detection
  # ---------------------------------------------------------------------------
  guess_xyz_by_names <- function(nm) {
    nm0 <- tolower(nm)
    xhit <- grepl("(^x$|accelx|accelerometerx|accel[-_ ]?x|axisx|xaxis|rawx)", nm0)
    yhit <- grepl("(^y$|accely|accelerometery|accel[-_ ]?y|axisy|yaxis|rawy)", nm0)
    zhit <- grepl("(^z$|accelz|accelerometerz|accel[-_ ]?z|axisz|zaxis|rawz)", nm0)
    list(x = which(xhit), y = which(yhit), z = which(zhit))
  }

  guess_time_by_names <- function(nm) {
    nm0 <- tolower(nm)
    which(grepl("time|timestamp|datetime|date_time|date|utc|epoch", nm0))
  }

  pick_time_col <- function(df, candidates_idx) {
    target_dt <- 1 / sample_rate
    best <- list(idx = NA_integer_, score = Inf, ok_rate = 0, spec = NULL)

    for (j in candidates_idx) {
      p <- parse_time_any(df, j, requested_tz = tz)
      tt <- p$time
      ok <- !is.na(tt)
      ok_rate <- mean(ok)
      if (!is.finite(ok_rate) || ok_rate < 0.80) next

      sc <- score_regular(as.numeric(tt[ok]), target_dt) + year_penalty(tt[ok])
      if (is.finite(sc) && sc < best$score) {
        best <- list(idx = j, score = sc, ok_rate = ok_rate, spec = p$spec)
      }
    }

    best
  }

  pick_xyz_cols <- function(df, exclude_idx = integer()) {
    num_idx <- which(vapply(df, function(x) is.numeric(x) || is.integer(x), logical(1)))
    num_idx <- setdiff(num_idx, exclude_idx)
    if (length(num_idx) < 3) return(NULL)

    n <- nrow(df)
    idx <- unique(round(seq(1, n, length.out = min(200000, n))))

    M <- as.matrix(df[idx, num_idx, drop = FALSE])

    keep <- which(colMeans(is.finite(M)) > 0.95)
    if (length(keep) < 3) return(NULL)
    M <- M[, keep, drop = FALSE]
    num_idx <- num_idx[keep]

    sds <- apply(M, 2, stats::sd, na.rm = TRUE)
    keep2 <- which(is.finite(sds) & sds > 1e-6)
    if (length(keep2) < 3) return(NULL)
    M <- M[, keep2, drop = FALSE]
    num_idx <- num_idx[keep2]

    K <- min(12, ncol(M))
    ord <- order(apply(M, 2, stats::var, na.rm = TRUE), decreasing = TRUE)
    M2 <- M[, ord[seq_len(K)], drop = FALSE]
    idx2 <- num_idx[ord[seq_len(K)]]

    best <- list(cols = NULL, score = Inf)
    combs <- utils::combn(seq_len(ncol(M2)), 3, simplify = FALSE)

    for (cc in combs) {
      A <- M2[, cc, drop = FALSE]
      s <- apply(A, 2, stats::sd, na.rm = TRUE)
      if (any(!is.finite(s))) next
      scale_pen <- stats::sd(log(s + 1e-12))

      R <- suppressWarnings(stats::cor(A, use = "pairwise.complete.obs"))
      if (any(!is.finite(R))) next
      off <- abs(R[lower.tri(R)])
      corr_pen <- mean(pmax(0, 0.05 - off) + pmax(0, off - 0.999))

      sc <- scale_pen + 5 * corr_pen
      if (is.finite(sc) && sc < best$score) {
        best <- list(cols = idx2[cc], score = sc)
      }
    }

    best$cols
  }

  detect_cols_robust <- function(df) {
    df <- as.data.frame(df)
    if (!is.null(names(df))) names(df) <- normalize_names(names(df))

    tc_exp <- resolve_col(df, time_col)
    xc_exp <- resolve_col(df, x_col)
    yc_exp <- resolve_col(df, y_col)
    zc_exp <- resolve_col(df, z_col)

    if (!is.null(tc_exp) && !is.null(xc_exp) && !is.null(yc_exp) && !is.null(zc_exp)) {
      return(list(time_col = tc_exp, x_col = xc_exp, y_col = yc_exp, z_col = zc_exp, df = df, time_pick = NULL))
    }

    nms <- names(df)
    has_names <- !is.null(nms) && all(nzchar(nms)) && !all(grepl("^v\\d+$", nms))

    if (has_names) {
      t_idx <- guess_time_by_names(nms)
      xyz <- guess_xyz_by_names(nms)

      tc <- tc_exp %||% if (length(t_idx)) nms[t_idx[1]] else NA_character_
      xc <- xc_exp %||% if (length(xyz$x)) nms[xyz$x[1]] else NA_character_
      yc <- yc_exp %||% if (length(xyz$y)) nms[xyz$y[1]] else NA_character_
      zc <- zc_exp %||% if (length(xyz$z)) nms[xyz$z[1]] else NA_character_

      if (anyNA(c(tc, xc, yc, zc))) {
        best <- pick_time_col(df, seq_along(df))
        if (!is.na(best$idx)) tc <- nms[best$idx]

        ex <- if (!is.na(best$idx)) best$idx else integer()
        xyz_idx <- pick_xyz_cols(df, exclude_idx = ex)
        if (!is.null(xyz_idx)) {
          cn <- nms[xyz_idx]
          vv <- vapply(df[xyz_idx], function(v) stats::var(as.numeric(v), na.rm = TRUE), numeric(1))
          ord <- order(vv, decreasing = TRUE)
          xc <- cn[ord[1]]
          yc <- cn[ord[2]]
          zc <- cn[ord[3]]
        }

        return(list(time_col = tc, x_col = xc, y_col = yc, z_col = zc, df = df, time_pick = best))
      }

      return(list(time_col = tc, x_col = xc, y_col = yc, z_col = zc, df = df, time_pick = NULL))
    }

    best <- pick_time_col(df, seq_along(df))
    if (is.na(best$idx)) {
      stop("GENERIC loader could not detect a credible time column.\n",
           "Tip: pass time_col explicitly.")
    }

    xyz_idx <- pick_xyz_cols(df, exclude_idx = best$idx)
    if (is.null(xyz_idx) || length(xyz_idx) != 3) {
      stop("GENERIC loader detected time column but could not detect 3 numeric axis columns.\n",
           "Tip: pass x_col/y_col/z_col explicitly.")
    }

    list(
      time_col = names(df)[best$idx],
      x_col = names(df)[xyz_idx[1]],
      y_col = names(df)[xyz_idx[2]],
      z_col = names(df)[xyz_idx[3]],
      df = df,
      time_pick = best
    )
  }

  # ---------------------------------------------------------------------------
  # file reading
  # ---------------------------------------------------------------------------
  read_any <- function(path, ext) {
    guess_header <- function(lines, sep) {
      if (length(lines) < 2) return(FALSE)

      split_line <- function(s) strsplit(s, split = sep, fixed = TRUE)[[1]]
      tok1 <- trimws(split_line(lines[1]))
      tok2 <- trimws(split_line(lines[2]))

      is_num <- function(x) {
        x <- gsub("\"", "", x)
        x <- trimws(x)
        grepl("^[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?$", x)
      }

      p1 <- mean(is_num(tok1))
      p2 <- mean(is_num(tok2))

      if (is.finite(p1) && is.finite(p2)) {
        if (p1 >= 0.8 && p2 >= 0.8) return(FALSE)
        if (p1 <= 0.2 && p2 >= 0.8) return(TRUE)
      }

      any(grepl("[A-Za-z_]", tok1) & !is_num(tok1))
    }

    if (ext %in% c("csv", "gz")) {
      if (!requireNamespace("data.table", quietly = TRUE)) {
        stop("Package 'data.table' is required to read CSV/CSV.GZ.")
      }

      delim_use <- delim
      if (is.null(delim_use)) {
        sniff <- readLines(path, n = 10, warn = FALSE)
        sniff <- paste(sniff, collapse = "\n")
        c_comma <- stringr::str_count(sniff, ",")
        c_semi  <- stringr::str_count(sniff, ";")
        c_tab   <- stringr::str_count(sniff, "\t")
        delim_use <- names(which.max(c("," = c_comma, ";" = c_semi, "\t" = c_tab)))
      }

      skip_n <- detect_data_start_row(path, n_lines = 2000)
      if (skip_n > 0) add_report(sprintf("Detected metadata block: skipped first %d lines.", skip_n))

      hdr_use <- header
      if (is.na(hdr_use)) {
        first2 <- readLines(path, n = 2 + skip_n, warn = FALSE)
        first2 <- tail(first2, 2)
        hdr_use <- guess_header(first2, delim_use)
      }

      df <- as.data.frame(data.table::fread(
        path,
        sep = delim_use,
        header = isTRUE(hdr_use),
        skip = skip_n,
        nrows = max_scan,
        showProgress = verbose,
        data.table = FALSE
      ))

      attr(df, ".generic_delim") <- delim_use
      attr(df, ".generic_header") <- isTRUE(hdr_use)
      attr(df, ".generic_skip") <- skip_n
      df
    } else if (ext == "rds") {
      obj <- readRDS(path)
      if (inherits(obj, "data.frame") || inherits(obj, "data.table")) as.data.frame(obj)
      else stop("RDS does not contain a data.frame-like object: ", basename(path))
    } else if (ext %in% c("rda", "rdata")) {
      env <- new.env(parent = emptyenv())
      nm <- load(path, envir = env)
      for (k in nm) {
        obj <- env[[k]]
        if (inherits(obj, "data.frame") || inherits(obj, "data.table")) return(as.data.frame(obj))
      }
      stop("RDA/RData contained no data.frame-like object: ", basename(path))
    } else if (ext %in% c("xlsx", "xls")) {
      if (!requireNamespace("readxl", quietly = TRUE)) stop("Package 'readxl' is required to read Excel files.")
      as.data.frame(readxl::read_excel(path, sheet = 1))
    } else if (ext %in% c("parquet", "feather")) {
      if (!requireNamespace("arrow", quietly = TRUE)) stop("Package 'arrow' is required to read Parquet/Feather.")
      if (ext == "parquet") as.data.frame(arrow::read_parquet(path)) else as.data.frame(arrow::read_feather(path))
    } else {
      stop("Unsupported file extension for device='generic': .", ext,
           "\nSupported: csv, csv.gz, rds, rda/RData, xlsx/xls, parquet, feather")
    }
  }

  reread_full_csv_if_needed <- function(path, scan_df) {
    if (!(ext %in% c("csv", "gz"))) return(scan_df)
    if (!requireNamespace("data.table", quietly = TRUE)) {
      stop("Package 'data.table' is required to read CSV/CSV.GZ.")
    }

    delim_use <- attr(scan_df, ".generic_delim") %||% ","
    hdr_use   <- attr(scan_df, ".generic_header") %||% TRUE
    skip_n    <- attr(scan_df, ".generic_skip") %||% 0L

    as.data.frame(data.table::fread(
      path,
      sep = delim_use,
      header = isTRUE(hdr_use),
      skip = skip_n,
      showProgress = verbose,
      data.table = FALSE
    ))
  }

  # ---------------------------------------------------------------------------
  # grid actions (NO interpolation)
  # ---------------------------------------------------------------------------
  snap_to_grid_and_collapse <- function(df, hz, compute_tz) {
    df <- df[order(df$time), , drop = FALSE]
    period <- 1 / hz
    t0 <- as.numeric(df$time[1])
    snapped <- round((as.numeric(df$time) - t0) / period) * period + t0
    df$time <- as.POSIXct(snapped, origin = "1970-01-01", tz = compute_tz)
    attr(df$time, "tzone") <- compute_tz

    out <- df |>
      dplyr::group_by(time) |>
      dplyr::summarise(
        X = mean(X, na.rm = TRUE),
        Y = mean(Y, na.rm = TRUE),
        Z = mean(Z, na.rm = TRUE),
        .groups = "drop"
      ) |>
      as.data.frame()

    out$time <- as.POSIXct(out$time, tz = compute_tz)
    attr(out$time, "tzone") <- compute_tz
    out
  }

  rebuild_grid_preserve_order <- function(df, hz, compute_tz) {
    df <- df[order(df$time), , drop = FALSE]
    n <- nrow(df)
    t0 <- as.POSIXct(df$time[1], tz = compute_tz)
    attr(t0, "tzone") <- compute_tz
    new_time <- t0 + seq(0, by = 1 / hz, length.out = n)
    attr(new_time, "tzone") <- compute_tz
    df$time <- new_time
    df
  }

  # ---------------------------------------------------------------------------
  # calibration helpers
  # ---------------------------------------------------------------------------
  .as_plain_xyz <- function(df, compute_tz) {
    df <- as.data.frame(df)
    df <- df[, intersect(names(df), c("time", "X", "Y", "Z")), drop = FALSE]
    flatten_num <- function(x) if (is.list(x)) as.numeric(unlist(x, use.names = FALSE)) else as.numeric(x)
    df$time <- as.POSIXct(df$time, tz = compute_tz)
    attr(df$time, "tzone") <- compute_tz
    df$X <- flatten_num(df$X)
    df$Y <- flatten_num(df$Y)
    df$Z <- flatten_num(df$Z)
    ok <- is.finite(df$X) & is.finite(df$Y) & is.finite(df$Z) & !is.na(df$time)
    df <- df[ok, , drop = FALSE]
    df[order(df$time), , drop = FALSE]
  }

  .has_zero_triplets <- function(df, eps = 0) {
    any((abs(df$X) <= eps) & (abs(df$Y) <= eps) & (abs(df$Z) <= eps), na.rm = TRUE)
  }

  .drop_zero_triplets <- function(df, eps = 0) {
    keep <- !((abs(df$X) <= eps) & (abs(df$Y) <= eps) & (abs(df$Z) <= eps))
    list(df = df[keep, , drop = FALSE], n_dropped = sum(!keep))
  }

  decide_units_auto <- function(X, Y, Z) {
    n <- length(X)
    if (!is.finite(n) || n < unit_guard_min_n) return("g")

    idx <- unique(round(seq(1, n, length.out = min(200000, n))))
    vm <- sqrt(X[idx]^2 + Y[idx]^2 + Z[idx]^2)
    vm <- vm[is.finite(vm)]
    if (length(vm) < 1000) return("g")

    med_vm <- stats::median(vm)

    if (med_vm > 2 && med_vm < 6) return("g")

    score_g   <- abs(med_vm - 1.0) / 1.0
    score_ms2 <- abs(med_vm - 9.80665) / 9.80665

    if (med_vm > 6 && (score_ms2 < unit_ratio_cutoff * score_g)) return("m/s2")
    "g"
  }

  convert_to_g <- function(df, units_choice) {
    if (units_choice == "m/s2") {
      df$X <- df$X / 9.80665
      df$Y <- df$Y / 9.80665
      df$Z <- df$Z / 9.80665
    }
    df
  }

  needs_autocal <- function(df_g, thr = cal_vm_dev_threshold) {
    vm <- sqrt(df_g$X^2 + df_g$Y^2 + df_g$Z^2)
    dev <- stats::median(abs(vm - 1), na.rm = TRUE)
    is.finite(dev) && dev > thr
  }

  # ---------------------------------------------------------------------------
  # read, detect, parse
  # ---------------------------------------------------------------------------
  scan_df <- read_any(file_path, ext)
  det <- detect_cols_robust(scan_df)

  df0 <- reread_full_csv_if_needed(file_path, scan_df)
  if (!identical(df0, scan_df)) det <- detect_cols_robust(df0)
  df0 <- det$df

  tc <- det$time_col
  xc <- det$x_col
  yc <- det$y_col
  zc <- det$z_col

  time_spec <- list(
    time_source_type = NA_character_,
    source_tz_detected = NA,
    source_tz = NA_character_,
    compute_tz = NA_character_,
    output_tz = tz,
    tz_rule = NA_character_,
    model = NA_character_,
    grid_action = "kept",
    units_choice = NA_character_,
    autocalibrate_setting = autocalibrate,
    autocalibrated = FALSE,
    autocalibrate_note = "not_attempted"
  )

  use_epoch_elapsed <- FALSE
  epoch_elapsed <- NULL

  p_main <- parse_time_any(df0, match(tc, names(df0)), requested_tz = tz)
  main_time <- p_main$time
  main_spec <- p_main$spec
  main_score <- main_spec$score %||% Inf

  if (is.null(time_col)) {
    epoch_elapsed <- detect_epoch_plus_elapsed(df0, target_dt = 1 / sample_rate)
    if (!is.null(epoch_elapsed)) {
      if (!is.finite(main_score) || epoch_elapsed$score + 5 < main_score) {
        use_epoch_elapsed <- TRUE
      }
    }
  }

  if (use_epoch_elapsed) {
    anchor_idx <- epoch_elapsed$anchor
    elapsed_idx <- epoch_elapsed$elapsed

    anchor_parse <- parse_time_numeric_best_utc(df0[[anchor_idx]], target_dt = 1 / sample_rate)
    anchor_utc <- anchor_parse$time_utc
    elapsed_raw <- suppressWarnings(as.numeric(df0[[elapsed_idx]]))
    ok <- is.finite(elapsed_raw) & !is.na(anchor_utc)

    if (!any(ok)) {
      time <- main_time
      time_spec <- utils::modifyList(time_spec, main_spec)
      add_report("Epoch+elapsed rescue was considered but could not be reconstructed; standard detected time column was used.")
    } else {
      e0 <- elapsed_raw[ok][1]
      delta_ticks <- elapsed_raw - e0
      unit_scale <- switch(epoch_elapsed$elapsed_unit,
                           ns = 1e-9, us = 1e-6, ms = 1e-3, s = 1, 1e-9)
      delta_sec <- delta_ticks * unit_scale
      time_utc <- anchor_utc + delta_sec
      time <- as.POSIXct(time_utc, tz = "UTC")
      attr(time, "tzone") <- "UTC"

      time_spec <- utils::modifyList(time_spec, list(
        time_source_type = paste0(epoch_elapsed$anchor_model, "+elapsed_", epoch_elapsed$elapsed_unit),
        source_tz_detected = TRUE,
        source_tz = "UTC",
        compute_tz = "UTC",
        output_tz = tz,
        tz_rule = "compute_in_source_tz",
        model = "epoch_plus_elapsed",
        anchor_col = names(df0)[anchor_idx],
        elapsed_col = names(df0)[elapsed_idx],
        elapsed_unit = epoch_elapsed$elapsed_unit,
        score = epoch_elapsed$score
      ))

      add_report(sprintf(
        "Detected dual time columns: anchor=%s (%s), elapsed=%s (%s).",
        names(df0)[anchor_idx], epoch_elapsed$anchor_model,
        names(df0)[elapsed_idx], epoch_elapsed$elapsed_unit
      ))
      add_report("Compute timeline uses source timezone characteristics. For epoch-style anchor/elapsed this is UTC.")
      add_report(sprintf("Requested output timezone for driver/export is %s.", tz))
    }
  } else {
    time <- main_time
    time_spec <- utils::modifyList(time_spec, main_spec)

    if (isTRUE(time_spec$source_tz_detected)) {
      add_report(sprintf(
        "Detected time column '%s' with source timezone information available.",
        tc
      ))
      add_report(sprintf(
        "Compute timeline uses source timezone = %s.",
        time_spec$compute_tz %||% time_spec$source_tz
      ))
    } else {
      add_report(sprintf(
        "Detected time column '%s' with time model '%s'.",
        tc, time_spec$model
      ))
      add_report(sprintf(
        "Raw timezone was not explicitly detectable. Compute timeline uses assumed source timezone = %s.",
        time_spec$compute_tz %||% tz
      ))
      add_report("Caution: verify that the automatically used timezone is appropriate for your data.")
    }
    add_report(sprintf("Requested output timezone for driver/export is %s.", tz))
  }

  compute_tz <- time_spec$compute_tz %||% "UTC"

  X <- suppressWarnings(as.numeric(df0[[xc]]))
  Y <- suppressWarnings(as.numeric(df0[[yc]]))
  Z <- suppressWarnings(as.numeric(df0[[zc]]))

  raw <- tibble::tibble(time = time, X = X, Y = Y, Z = Z) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  if (!nrow(raw)) stop("No samples after read: ", basename(file_path))

  first_year <- suppressWarnings(as.integer(format(raw$time[1], "%Y")))
  if (is.finite(first_year) && first_year == 1970 && !identical(compute_tz, "UTC")) {
    stop(
      "GENERIC loader detected a timeline anchored to 1970-01-01.\n",
      "This usually means an elapsed/time-of-day column was selected without a credible calendar date anchor.\n",
      "Pass time_col explicitly or verify the raw file contains an absolute timestamp column."
    )
  }

  add_report(sprintf(
    "Columns selected: time=%s, X=%s, Y=%s, Z=%s. Rows kept after filtering=%d.",
    tc, xc, yc, zc, nrow(raw)
  ))
  add_report(sprintf(
    "Timeline span after parsing (compute tz): first=%s ; last=%s",
    format(raw$time[1], tz = compute_tz, usetz = TRUE),
    format(raw$time[nrow(raw)], tz = compute_tz, usetz = TRUE)
  ))

  # ---------------------------------------------------------------------------
  # grid handling (NO interpolation)
  # ---------------------------------------------------------------------------
  if (is_regular_enough(raw$time, hz = sample_rate)) {
    time_spec$grid_action <- "kept"
    add_report(sprintf("Timeline regularity check passed for %g Hz. Original compute timeline retained.", sample_rate))
  } else {
    add_report(sprintf(
      "Timeline irregular/unsafe for declared %g Hz. Applying snap-to-grid with duplicate collapse first (no interpolation).",
      sample_rate
    ))

    snapped <- snap_to_grid_and_collapse(raw, hz = sample_rate, compute_tz = compute_tz)
    if (is_regular_enough(snapped$time, hz = sample_rate)) {
      raw <- tibble::as_tibble(snapped)
      time_spec$grid_action <- "snapped"
      add_report("Grid action used: snapped. Timestamps were rounded to nearest grid tick and duplicate ticks were averaged.")
    } else {
      raw <- tibble::as_tibble(rebuild_grid_preserve_order(raw, hz = sample_rate, compute_tz = compute_tz))
      time_spec$grid_action <- "rebuilt"
      add_report("Grid action used: rebuilt. A strict time index of length N was rebuilt preserving sample order.")
      add_report("Important: this does not interpolate missing values; it only reindexes observed samples.")
    }
  }

  # ---------------------------------------------------------------------------
  # units
  # ---------------------------------------------------------------------------
  units_choice <- if (units == "auto") decide_units_auto(raw$X, raw$Y, raw$Z) else units
  raw <- convert_to_g(raw, units_choice)
  time_spec$units_choice <- units_choice
  add_report(sprintf("Units handling: requested=%s, used=%s.", units, units_choice))

  # ---------------------------------------------------------------------------
  # autocalibration
  # ---------------------------------------------------------------------------
  do_cal <- switch(
    autocalibrate,
    "true"  = TRUE,
    "false" = FALSE,
    "auto"  = needs_autocal(raw)
  )
  add_report(sprintf("Autocalibration: setting=%s, decision=%s.", autocalibrate, if (do_cal) "YES" else "NO"))

  if (do_cal && !requireNamespace("agcounts", quietly = TRUE)) {
    do_cal <- FALSE
    time_spec$autocalibrated <- FALSE
    time_spec$autocalibrate_note <- "agcounts_missing"
    add_report("Autocalibration could not run because package 'agcounts' is not installed. Proceeding uncalibrated.")
  }

  if (do_cal) {
    raw2 <- .as_plain_xyz(raw, compute_tz = compute_tz)
    cal <- NULL
    cal_err <- NULL

    tryCatch({
      cal <- agcounts::agcalibrate(raw = raw2, verbose = FALSE, tz = compute_tz)
    }, error = function(e) cal_err <<- e)

    if (is.null(cal)) {
      msg0 <- if (!is.null(cal_err)) conditionMessage(cal_err) else "Unknown error"

      if (.has_zero_triplets(raw2, eps = zero_triplet_eps)) {
        z <- .drop_zero_triplets(raw2, eps = zero_triplet_eps)
        add_report(sprintf(
          "Calibration retry: removed %d zero-triplet rows (%.3f%%).",
          z$n_dropped, 100 * z$n_dropped / nrow(raw2)
        ))
        cal <- tryCatch(
          agcounts::agcalibrate(raw = z$df, verbose = FALSE, tz = compute_tz),
          error = function(e) { cal_err <<- e; NULL }
        )
      }

      if (is.null(cal)) {
        msg1 <- if (!is.null(cal_err)) conditionMessage(cal_err) else msg0
        time_spec$autocalibrated <- FALSE
        time_spec$autocalibrate_note <- paste0("failed:", msg1)
        add_report(paste0("Autocalibration failed. Proceeding uncalibrated. Reason: ", msg1))
        cal <- raw2
      } else {
        time_spec$autocalibrated <- TRUE
        time_spec$autocalibrate_note <- "ok_after_zero_drop"
        add_report("Autocalibration succeeded after retry.")
      }
    } else {
      time_spec$autocalibrated <- TRUE
      time_spec$autocalibrate_note <- "ok"
      add_report("Autocalibration succeeded.")
    }

    cal <- as.data.frame(cal)
    if (!all(c("time", "X", "Y", "Z") %in% names(cal))) {
      time_spec$autocalibrated <- FALSE
      time_spec$autocalibrate_note <- "bad_cal_output"
      add_report("Autocalibration output did not contain required columns. Proceeding uncalibrated.")
      cal <- raw2
    }

    raw <- tibble::tibble(
      time = as.POSIXct(cal$time, tz = compute_tz),
      X = as.numeric(cal$X),
      Y = as.numeric(cal$Y),
      Z = as.numeric(cal$Z)
    ) |>
      dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
      dplyr::arrange(time)

    attr(raw$time, "tzone") <- compute_tz

    if (!nrow(raw)) stop("No samples after calibration fallback: ", basename(file_path))
  }

  # ---------------------------------------------------------------------------
  # final strict output grid (compute timeline only)
  # ---------------------------------------------------------------------------
  n <- nrow(raw)
  t0 <- lubridate::floor_date(raw$time[1], "second")
  t0 <- as.POSIXct(t0, tz = compute_tz)
  attr(t0, "tzone") <- compute_tz

  out <- tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = raw$X,
    Y = raw$Y,
    Z = raw$Z
  )
  attr(out$time, "tzone") <- compute_tz

  add_report(sprintf(
    "Final compute timeline: first=%s ; last=%s ; compute_tz=%s",
    format(out$time[1], tz = compute_tz, usetz = TRUE),
    format(out$time[nrow(out)], tz = compute_tz, usetz = TRUE),
    compute_tz
  ))
  add_report(sprintf(
    "Requested output timezone retained for driver/export: %s",
    tz
  ))

  attr(out, "ua_time_spec") <- time_spec
  attr(out, "ua_time_report") <- unique(.time_report)

  out
}
