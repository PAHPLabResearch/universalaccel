# R/generic_loader.R
#' Read regularly-sampled accelerometer data from common tabular formats
#' (csv/csv.gz, rds, rda/RData, xlsx/xls, parquet/feather if available),
#' robustly detect time + X/Y/Z columns, optionally auto-calibrate via
#' agcounts::agcalibrate(), and return tibble(time,X,Y,Z) on a strict grid.
#'
#' Robustness goals:
#' - Accept headered OR headerless delimited files.
#' - Detect time stored as:
#'   * POSIX-like strings (including year=1111 and other weird years)
#'   * Unix seconds / milliseconds / microseconds / nanoseconds
#'   * Excel serial dates
#'   * Time-of-day strings without date (reconstruct)
#' - Repair common broken time strings:
#'   * "YYYY-mm-dd HH:MM.SSS"   (missing seconds)
#'   * "YYYY-mm-dd HH:MM:SS"    (missing milliseconds)
#'   * "YYYY-mm-dd"             (date only)
#'   * "HH:MM:SS.SSS"           (time only)
#'   * "HH:MM.SSS"              (missing seconds)
#' - Detect X/Y/Z among many numeric columns using name hints + content-based scoring.
#' - Ignore extra columns silently once time/X/Y/Z are identified.
#'
#' Notes:
#' - Assumes data are regularly sampled; final output is re-gridded to sample_rate.
#' - Autocalibration is "best-effort": if it fails (for ANY reason), it falls back
#'   to uncalibrated signals (optionally verbose message), EVEN if autocalibrate=="true".
#' - COUNTS 30–100 Hz restriction is NOT applied here (belongs to calculate_counts).
#'
#' @noRd
read_and_calibrate_generic <- function(file_path,
                                       sample_rate = 100,
                                       tz = "UTC",
                                       autocalibrate = c("auto", "true", "false"),
                                       units = c("auto", "g", "m/s2"),
                                       # unit detection guardrails
                                       unit_guard_min_n = 5000,
                                       unit_ratio_cutoff = 0.35,
                                       # auto-cal heuristic (in g)
                                       cal_vm_dev_threshold = 0.03,
                                       # imputed-zero handling for calibration
                                       zero_triplet_eps = 0,
                                       # optional explicit column mapping
                                       time_col = NULL,
                                       x_col = NULL,
                                       y_col = NULL,
                                       z_col = NULL,
                                       # reading options
                                       delim = NULL,          # NULL -> auto for CSV-like
                                       header = NA,           # NA -> auto detect
                                       max_scan = 200000,     # rows to scan for detection
                                       verbose = FALSE) {

  autocalibrate <- match.arg(tolower(autocalibrate), choices = c("auto","true","false"))
  units <- match.arg(units)

  stopifnot(is.finite(sample_rate), sample_rate > 0)

  # deps
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Package 'stringr' is required.")
  if (!requireNamespace("tibble", quietly = TRUE)) stop("Package 'tibble' is required.")

  file_path <- normalizePath(file_path, winslash = "/", mustWork = TRUE)
  ext <- tolower(tools::file_ext(file_path))

  # ---------------- helpers ----------------
  vmsg <- function(...) if (isTRUE(verbose)) message(...)

  normalize_names <- function(nm) {
    nm <- gsub("\u00A0", " ", nm, perl = TRUE)
    nm <- trimws(nm)
    nm <- tolower(nm)
    nm <- gsub("[^a-z0-9]+", "_", nm)
    nm <- gsub("^_+|_+$", "", nm)
    nm
  }

  # --- Timestamp repair for MANY broken patterns ---
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

    # keep ms precision; pad/trim to 3 decimals
    s <- sub("^(.*\\.[0-9]{3})[0-9]+$", "\\1", s, perl = TRUE)
    s <- sub("^(.*\\.[0-9]{2})$", "\\10", s, perl = TRUE)
    s <- sub("^(.*\\.[0-9]{1})$", "\\100", s, perl = TRUE)

    s
  }

  parse_time_string <- function(x_chr) {
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
      tz = tz, exact = FALSE
    ))
  }

  score_regular <- function(tt, target_dt) {
    tt <- tt[is.finite(tt)]
    if (length(tt) < 50) return(Inf)
    d <- diff(as.numeric(tt))
    d <- d[is.finite(d) & d > 0]
    if (!length(d)) return(Inf)
    med <- stats::median(d)
    mad <- stats::median(abs(d - med))
    abs(med - target_dt) + mad
  }

  parse_time_numeric_best <- function(x_num, target_dt = 1 / sample_rate) {
    x0 <- suppressWarnings(as.numeric(x_num))
    if (all(is.na(x0))) return(as.POSIXct(rep(NA_real_, length(x_num)), origin = "1970-01-01", tz = tz))

    x <- x0[is.finite(x0)]
    if (length(x) < 50) return(as.POSIXct(rep(NA_real_, length(x_num)), origin = "1970-01-01", tz = tz))

    cand <- list(
      unix_s  = function(v) as.POSIXct(v, origin = "1970-01-01", tz = tz),
      unix_ms = function(v) as.POSIXct(v / 1e3, origin = "1970-01-01", tz = tz),
      unix_us = function(v) as.POSIXct(v / 1e6, origin = "1970-01-01", tz = tz),
      unix_ns = function(v) as.POSIXct(v / 1e9, origin = "1970-01-01", tz = tz),
      excel   = function(v) as.POSIXct((v - 25569) * 86400, origin = "1970-01-01", tz = tz)
    )

    plaus_penalty <- function(tt) {
      yr <- suppressWarnings(as.integer(format(tt[1], "%Y")))
      if (is.na(yr)) return(1e6)
      if (yr < 1950 || yr > 2100) return(500) else 0
    }

    scores <- vapply(names(cand), function(nm) {
      tt <- try(cand[[nm]](x), silent = TRUE)
      if (inherits(tt, "try-error")) return(Inf)
      score_regular(tt, target_dt) + plaus_penalty(tt)
    }, numeric(1))

    best <- names(scores)[which.min(scores)]
    tt_all <- cand[[best]](x0)
    vmsg("[GENERIC] numeric time parse chose: ", best, " (score=", signif(min(scores), 4), ")")
    tt_all
  }

  parse_time_of_day_only <- function(x_chr) {
    s <- canon_ts_chr(x_chr)
    is_tod <- stringr::str_detect(s, "^\\d{1,2}:\\d{2}:\\d{2}\\.\\d{3}$")
    if (!any(is_tod)) return(NULL)

    date_hit <- stringr::str_extract(s, "\\d{4}-\\d{2}-\\d{2}")
    anchor <- date_hit[which(!is.na(date_hit))[1]]
    if (is.na(anchor)) anchor <- "1970-01-01"

    tod <- s[is_tod]
    out <- as.POSIXct(paste(anchor, tod), tz = tz, format = "%Y-%m-%d %H:%M:%OS3")
    full <- rep(as.POSIXct(NA_real_, origin = "1970-01-01", tz = tz), length(s))
    full[is_tod] <- out
    full
  }

  parse_time_any <- function(x) {
    if (inherits(x, "POSIXt")) return(lubridate::force_tz(as.POSIXct(x), tz))
    if (is.numeric(x) || is.integer(x)) return(parse_time_numeric_best(x, target_dt = 1 / sample_rate))

    x_chr <- as.character(x)

    tod <- parse_time_of_day_only(x_chr)
    if (!is.null(tod) && any(!is.na(tod))) return(tod)

    out <- parse_time_string(x_chr)

    if (all(is.na(out))) {
      num <- suppressWarnings(as.numeric(x_chr))
      if (!all(is.na(num))) return(parse_time_numeric_best(num, target_dt = 1 / sample_rate))
    }
    out
  }

  decide_units_auto <- function(X, Y, Z) {
    n <- length(X)
    if (!is.finite(n) || n < unit_guard_min_n) {
      vmsg("[GENERIC] units=auto but n<", unit_guard_min_n, " -> defaulting to g")
      return("g")
    }

    idx <- unique(round(seq(1, n, length.out = min(200000, n))))
    vm <- sqrt(X[idx]^2 + Y[idx]^2 + Z[idx]^2)
    vm <- vm[is.finite(vm)]
    if (length(vm) < 1000) return("g")

    med_vm <- stats::median(vm)

    if (med_vm > 2 && med_vm < 6) {
      vmsg("[GENERIC] units=auto gray zone (median VM=", signif(med_vm,4), ") -> g")
      return("g")
    }

    score_g   <- abs(med_vm - 1.0) / 1.0
    score_ms2 <- abs(med_vm - 9.80665) / 9.80665

    if (med_vm > 6 && (score_ms2 < unit_ratio_cutoff * score_g)) {
      vmsg("[GENERIC] units=auto -> m/s2 (median VM=", signif(med_vm,4), ")")
      return("m/s2")
    }

    vmsg("[GENERIC] units=auto -> g (median VM=", signif(med_vm,4), ")")
    "g"
  }

  convert_to_g <- function(df, units_choice) {
    if (units_choice == "m/s2") {
      vmsg("[GENERIC] converting m/s^2 -> g by / 9.80665")
      df$X <- df$X / 9.80665
      df$Y <- df$Y / 9.80665
      df$Z <- df$Z / 9.80665
    }
    df
  }

  needs_autocal <- function(df_g, thr = cal_vm_dev_threshold) {
    vm <- sqrt(df_g$X^2 + df_g$Y^2 + df_g$Z^2)
    dev <- stats::median(abs(vm - 1), na.rm = TRUE)
    vmsg("[GENERIC] median(|VM-1|)=", signif(dev,4), " threshold=", thr)
    is.finite(dev) && dev > thr
  }

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
    best <- list(idx = NA_integer_, score = Inf, ok_rate = 0)

    for (j in candidates_idx) {
      v <- df[[j]]
      tt <- parse_time_any(v)
      ok <- !is.na(tt)
      ok_rate <- mean(ok)
      if (!is.finite(ok_rate) || ok_rate < 0.80) next

      sc <- score_regular(tt[ok], target_dt)
      if (is.finite(sc) && sc < best$score) best <- list(idx = j, score = sc, ok_rate = ok_rate)
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
      if (is.finite(sc) && sc < best$score) best <- list(cols = idx2[cc], score = sc)
    }

    best$cols
  }

  detect_cols_robust <- function(df) {
    df <- as.data.frame(df)
    if (!is.null(names(df))) names(df) <- normalize_names(names(df))

    if (!is.null(time_col) && !is.null(x_col) && !is.null(y_col) && !is.null(z_col)) {
      miss <- setdiff(c(time_col, x_col, y_col, z_col), names(df))
      if (length(miss)) stop("Explicit columns not found: ", paste(miss, collapse = ", "),
                             "\nAvailable: ", paste(names(df), collapse = ", "))
      return(list(time_col = time_col, x_col = x_col, y_col = y_col, z_col = z_col, df = df))
    }

    nms <- names(df)
    has_names <- !is.null(nms) && all(nzchar(nms)) && !all(grepl("^v\\d+$", nms))

    if (has_names) {
      t_idx <- guess_time_by_names(nms)
      xyz <- guess_xyz_by_names(nms)

      tc <- if (!is.null(time_col)) time_col else if (length(t_idx)) nms[t_idx[1]] else NA_character_
      xc <- if (!is.null(x_col)) x_col else if (length(xyz$x)) nms[xyz$x[1]] else NA_character_
      yc <- if (!is.null(y_col)) y_col else if (length(xyz$y)) nms[xyz$y[1]] else NA_character_
      zc <- if (!is.null(z_col)) z_col else if (length(xyz$z)) nms[xyz$z[1]] else NA_character_

      if (anyNA(c(tc, xc, yc, zc))) {
        best <- pick_time_col(df, seq_along(df))
        if (!is.na(best$idx)) tc <- nms[best$idx]

        ex <- if (!is.na(best$idx)) best$idx else integer()
        xyz_idx <- pick_xyz_cols(df, exclude_idx = ex)
        if (!is.null(xyz_idx)) {
          cn <- nms[xyz_idx]
          vv <- vapply(df[xyz_idx], function(v) stats::var(as.numeric(v), na.rm = TRUE), numeric(1))
          ord <- order(vv, decreasing = TRUE)
          xc <- cn[ord[1]]; yc <- cn[ord[2]]; zc <- cn[ord[3]]
        }
      }

      if (anyNA(c(tc, xc, yc, zc))) {
        stop("GENERIC loader could not identify required columns (time, X, Y, Z).\n",
             "Found columns: ", paste(nms, collapse = ", "), "\n",
             "Tip: pass time_col/x_col/y_col/z_col explicitly.")
      }
      return(list(time_col = tc, x_col = xc, y_col = yc, z_col = zc, df = df))
    }

    best <- pick_time_col(df, seq_along(df))
    if (is.na(best$idx)) {
      stop("GENERIC loader could not detect a time column in a headerless/unnamed file.\n",
           "Tip: pass time_col explicitly (e.g., time_col='v1' or 'v2').")
    }
    xyz_idx <- pick_xyz_cols(df, exclude_idx = best$idx)
    if (is.null(xyz_idx) || length(xyz_idx) != 3) {
      stop("GENERIC loader detected time column but could not detect 3 numeric axis columns.\n",
           "Tip: pass x_col/y_col/z_col explicitly.")
    }
    list(time_col = names(df)[best$idx],
         x_col = names(df)[xyz_idx[1]],
         y_col = names(df)[xyz_idx[2]],
         z_col = names(df)[xyz_idx[3]],
         df = df)
  }

  # -------------- reader --------------
  read_any <- function(path, ext) {

    # robust header guess that does NOT treat scientific notation "e/E" as header text
    guess_header <- function(lines, sep) {
      if (length(lines) < 2) return(FALSE)

      split_line <- function(s) strsplit(s, split = sep, fixed = TRUE)[[1]]
      tok1 <- trimws(split_line(lines[1]))
      tok2 <- trimws(split_line(lines[2]))

      # numeric token regex (supports scientific notation)
      is_num <- function(x) {
        x <- gsub("\"", "", x)
        x <- trimws(x)
        grepl("^[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?$", x)
      }

      p1 <- mean(is_num(tok1))
      p2 <- mean(is_num(tok2))

      if (is.finite(p1) && is.finite(p2)) {
        if (p1 >= 0.8 && p2 >= 0.8) return(FALSE) # data-like then data-like
        if (p1 <= 0.2 && p2 >= 0.8) return(TRUE)  # name-like then data-like
      }

      # fallback: any obvious name-like token on line1
      any(grepl("[A-Za-z_]", tok1) & !is_num(tok1))
    }

    if (ext %in% c("csv", "gz")) {
      if (!requireNamespace("data.table", quietly = TRUE))
        stop("Package 'data.table' is required to read CSV/CSV.GZ.")

      # delimiter sniff
      delim_use <- delim
      if (is.null(delim_use)) {
        sniff <- readLines(path, n = 5, warn = FALSE)
        sniff <- paste(sniff, collapse = "\n")
        c_comma <- stringr::str_count(sniff, ",")
        c_semi  <- stringr::str_count(sniff, ";")
        c_tab   <- stringr::str_count(sniff, "\t")
        delim2  <- c("," = c_comma, ";" = c_semi, "\t" = c_tab)
        delim_use <- names(delim2)[which.max(delim2)]
      }

      # header guess (robust)
      hdr_use <- header
      if (is.na(hdr_use)) {
        first2 <- readLines(path, n = 2, warn = FALSE)
        hdr_use <- guess_header(first2, delim_use)
      }

      df <- as.data.frame(data.table::fread(
        path,
        sep = delim_use,
        header = isTRUE(hdr_use),
        nrows = max_scan,
        showProgress = verbose,
        data.table = FALSE
      ))

      attr(df, ".generic_delim") <- delim_use
      attr(df, ".generic_header") <- isTRUE(hdr_use)
      return(df)
    }

    if (ext == "rds") {
      obj <- readRDS(path)
      if (inherits(obj, "data.frame") || inherits(obj, "data.table")) return(as.data.frame(obj))
      stop("RDS does not contain a data.frame-like object: ", basename(path))
    }

    if (ext %in% c("rda", "rdata")) {
      env <- new.env(parent = emptyenv())
      nm <- load(path, envir = env)
      for (k in nm) {
        obj <- env[[k]]
        if (inherits(obj, "data.frame") || inherits(obj, "data.table")) return(as.data.frame(obj))
      }
      stop("RDA/RData contained no data.frame-like object: ", basename(path))
    }

    if (ext %in% c("xlsx", "xls")) {
      if (!requireNamespace("readxl", quietly = TRUE))
        stop("Package 'readxl' is required to read Excel files.")
      return(as.data.frame(readxl::read_excel(path, sheet = 1)))
    }

    if (ext %in% c("parquet", "feather")) {
      if (!requireNamespace("arrow", quietly = TRUE))
        stop("Package 'arrow' is required to read Parquet/Feather.")
      if (ext == "parquet") return(as.data.frame(arrow::read_parquet(path)))
      return(as.data.frame(arrow::read_feather(path)))
    }

    stop("Unsupported file extension for device='generic': .", ext,
         "\nSupported: csv, csv.gz, rds, rda/RData, xlsx/xls, parquet, feather")
  }

  reread_full_csv_if_needed <- function(path, scan_df) {
    if (!(ext %in% c("csv", "gz"))) return(scan_df)
    if (!requireNamespace("data.table", quietly = TRUE))
      stop("Package 'data.table' is required to read CSV/CSV.GZ.")

    delim_use <- attr(scan_df, ".generic_delim"); if (is.null(delim_use)) delim_use <- ","
    hdr_use   <- attr(scan_df, ".generic_header"); if (is.null(hdr_use)) hdr_use <- TRUE

    as.data.frame(data.table::fread(
      path,
      sep = delim_use,
      header = isTRUE(hdr_use),
      showProgress = verbose,
      data.table = FALSE
    ))
  }

  # ---- HPC-style helpers for agcalibrate stability ----
  .as_plain_xyz <- function(df) {
    df <- as.data.frame(df)
    df <- df[, intersect(names(df), c("time","X","Y","Z")), drop = FALSE]
    flatten_num <- function(x) if (is.list(x)) as.numeric(unlist(x, use.names = FALSE)) else as.numeric(x)
    if (!inherits(df$time, "POSIXct")) df$time <- as.POSIXct(df$time, tz = tz)
    df$X <- flatten_num(df$X); df$Y <- flatten_num(df$Y); df$Z <- flatten_num(df$Z)
    ok <- is.finite(df$X) & is.finite(df$Y) & is.finite(df$Z) & !is.na(df$time)
    df <- df[ok, , drop = FALSE]
    df[order(df$time), , drop = FALSE]
  }

  .normalize_timeline <- function(df, target_hz) {
    if (!nrow(df)) return(df)
    df <- df[order(df$time), , drop = FALSE]
    period <- 1 / as.numeric(target_hz)
    t0     <- as.numeric(df$time[1])
    snapped <- round((as.numeric(df$time) - t0) / period) * period + t0
    tz0 <- attr(df$time, "tzone"); if (is.null(tz0)) tz0 <- tz
    df$time <- as.POSIXct(snapped, origin = "1970-01-01", tz = tz0)

    df |>
      dplyr::group_by(time) |>
      dplyr::summarise(
        X = mean(X, na.rm = TRUE),
        Y = mean(Y, na.rm = TRUE),
        Z = mean(Z, na.rm = TRUE),
        .groups = "drop"
      ) |>
      as.data.frame()
  }

  .has_zero_triplets <- function(df, eps = 0) {
    any((abs(df$X) <= eps) & (abs(df$Y) <= eps) & (abs(df$Z) <= eps), na.rm = TRUE)
  }

  .drop_zero_triplets <- function(df, eps = 0) {
    keep <- !((abs(df$X) <= eps) & (abs(df$Y) <= eps) & (abs(df$Z) <= eps))
    list(df = df[keep, , drop = FALSE], n_dropped = sum(!keep))
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

  rebuild_grid_from_first <- function(t0, n, hz) {
    t0 <- as.POSIXct(t0, tz = tz)
    t0 + seq(0, by = 1 / hz, length.out = n)
  }

  # ---------------- run ----------------
  scan_df <- read_any(file_path, ext)
  det <- detect_cols_robust(scan_df)

  df0 <- reread_full_csv_if_needed(file_path, scan_df)
  if (!identical(df0, scan_df)) det <- detect_cols_robust(df0)

  tc <- det$time_col; xc <- det$x_col; yc <- det$y_col; zc <- det$z_col
  df0 <- det$df

  time <- parse_time_any(df0[[tc]])
  X <- suppressWarnings(as.numeric(df0[[xc]]))
  Y <- suppressWarnings(as.numeric(df0[[yc]]))
  Z <- suppressWarnings(as.numeric(df0[[zc]]))

  raw <- tibble::tibble(time = time, X = X, Y = Y, Z = Z) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  if (!nrow(raw)) stop("No samples after read: ", basename(file_path))

  if (verbose) {
    message("[GENERIC] columns: time=", tc, " X=", xc, " Y=", yc, " Z=", zc,
            " n=", nrow(raw), " (file=", basename(file_path), ")")
  }

  if (!is_regular_enough(raw$time, hz = sample_rate)) {
    vmsg("[GENERIC] irregular/unsafe timestamping detected -> rebuilding regular grid from first valid time")
    raw$time <- rebuild_grid_from_first(raw$time[1], nrow(raw), hz = sample_rate)
  }

  units_choice <- units
  if (units == "auto") units_choice <- decide_units_auto(raw$X, raw$Y, raw$Z)
  raw <- convert_to_g(raw, units_choice)

  do_cal <- switch(
    autocalibrate,
    "true"  = TRUE,
    "false" = FALSE,
    "auto"  = needs_autocal(raw)
  )

  if (do_cal) {
    if (!requireNamespace("agcounts", quietly = TRUE)) {
      message("[GENERIC] agcounts not installed -> proceeding UNCALIBRATED")
      do_cal <- FALSE
    }
  }

  if (do_cal) {
    raw2 <- .as_plain_xyz(raw)
    raw2 <- .normalize_timeline(raw2, target_hz = sample_rate)

    vmsg("[GENERIC] applying agcounts::agcalibrate() (best-effort)")

    cal <- NULL
    cal_err <- NULL

    tryCatch({
      cal <- agcounts::agcalibrate(raw = raw2, verbose = FALSE, tz = tz)
    }, error = function(e) cal_err <<- e)

    if (is.null(cal)) {
      msg0 <- if (!is.null(cal_err)) conditionMessage(cal_err) else "Unknown error"

      if (grepl("imputed zero", msg0, ignore.case = TRUE) || .has_zero_triplets(raw2, eps = zero_triplet_eps)) {
        z <- .drop_zero_triplets(raw2, eps = zero_triplet_eps)
        vmsg(sprintf("[GENERIC] removed %d zero-triplet rows (%.3f%%), retry calibrate",
                     z$n_dropped, 100 * z$n_dropped / nrow(raw2)))
        cal <- tryCatch(
          agcounts::agcalibrate(raw = z$df, verbose = FALSE, tz = tz),
          error = function(e) { cal_err <<- e; NULL }
        )
      }

      if (is.null(cal)) {
        msg1 <- if (!is.null(cal_err)) conditionMessage(cal_err) else msg0
        message("[GENERIC] agcalibrate failed -> proceeding UNCALIBRATED: ", msg1)
        cal <- raw2
      }
    }

    cal <- as.data.frame(cal)
    if (!all(c("time","X","Y","Z") %in% names(cal))) {
      message("[GENERIC] calibrated output missing required columns -> proceeding UNCALIBRATED")
      cal <- raw2
    }

    raw <- tibble::as_tibble(cal) |>
      dplyr::transmute(
        time = lubridate::force_tz(as.POSIXct(time), tz),
        X = as.numeric(X), Y = as.numeric(Y), Z = as.numeric(Z)
      ) |>
      dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
      dplyr::arrange(time)

    if (!nrow(raw)) stop("No samples after calibration fallback: ", basename(file_path))
  } else {
    vmsg("[GENERIC] skipping autocalibration")
  }

  n  <- nrow(raw)
  t0 <- lubridate::floor_date(raw$time[1], "second")

  tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = raw$X, Y = raw$Y, Z = raw$Z
  )
}
