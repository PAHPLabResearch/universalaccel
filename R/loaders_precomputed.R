# R/loaders_precomputed.R

#' Load precomputed metric time-series for binning / IG (robust messy inputs)
#'
#' Reads a file into a canonical wide table with columns like:
#'   id, time, epoch_sec, ac, enmo, mad, mims_unit, ai, rocam, ...
#' plus optional: sex (M/F/All), age (integer)
#'
#' Supported inputs:
#'   - .csv / .txt (data.table::fread)
#'   - .xlsx / .xls (readxl::read_excel; first sheet)
#'   - .rds (readRDS; data.frame/data.table)
#'
#' HARD RULE:
#'   epoch_sec must be exactly 5 or 60 (and not mixed).
#'
#' Optional validity filtering (recommended):
#'   - Valid day  = >= valid_day_hours (default 16h) worth of records
#'   - Valid week = >= valid_week_days (default 3) valid days within a 7-day block
#'     where week blocks are defined per-id starting at the first observed day in the file.
#'
#' Notes:
#'   - If sex/age are missing, the loader will warn and fall back to sex='All' and age=NA.
#'   - Single-metric datasets are supported (must include at least one numeric metric column).
#'   - Adds attribute attr(DT,"ua_loader_meta") used downstream for Output5.
#'
#' @param path File path
#' @param apply_valid_filters logical; apply valid day/week rules
#' @param valid_day_hours numeric; default 16
#' @param valid_week_days integer; default 3
#' @export
load_precomputed_metrics <- function(path,
                                     apply_valid_filters = TRUE,
                                     valid_day_hours = 16,
                                     valid_week_days = 3) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Package 'data.table' is required.")
  if (!file.exists(path)) stop("File not found: ", path)

  ext <- tolower(tools::file_ext(path))

  # -----------------------------
  # read (csv/xlsx/rds)
  # -----------------------------
  if (ext %in% c("csv", "txt")) {
    DT <- data.table::fread(path, showProgress = FALSE)
  } else if (ext %in% c("xlsx", "xls")) {
    if (!requireNamespace("readxl", quietly = TRUE)) {
      stop("Excel input requires package 'readxl'. Install with install.packages('readxl').")
    }
    sheets <- readxl::excel_sheets(path)
    if (!length(sheets)) stop("Excel file has no sheets: ", path)
    DT <- readxl::read_excel(path, sheet = sheets[1], .name_repair = "unique")
  } else if (ext == "rds") {
    DT <- readRDS(path)
  } else {
    stop("Unsupported file type: .", ext, " (allowed: csv/txt, xlsx/xls, rds)")
  }

  data.table::setDT(DT)
  if (!nrow(DT)) stop("File has 0 rows: ", path)

  # -----------------------------
  # normalize names
  # -----------------------------
  nm0 <- names(DT)
  nm  <- tolower(nm0)
  nm  <- gsub("[^a-z0-9]+", "_", nm)
  nm  <- gsub("_+", "_", nm)
  nm  <- gsub("^_+|_+$", "", nm)
  nm[nm == ""] <- paste0("x", which(nm == ""))
  nm <- make.unique(nm, sep = "_")
  data.table::setnames(DT, nm0, nm)

  # -----------------------------
  # map synonyms -> canonical
  # -----------------------------
  map <- c(
    # id/time
    "time"="time", "datetime"="time", "timestamp"="time", "date_time"="time", "date"="time",
    "id"="id", "participant"="id", "subject"="id", "record_id"="id", "seqn"="id",

    # demographics
    "sex"="sex", "gender"="sex", "sex_name"="sex", "riagendr"="sex",
    "age"="age", "age_years"="age", "ridageyr"="age",

    # metrics
    "mims"="mims_unit", "mims_unit"="mims_unit",
    "ai"="ai",
    "enmo"="enmo",
    "mad"="mad",
    "rocam"="rocam",

    # AC/counts
    "vector_magnitude"="ac", "vector_magnitude_counts"="ac",
    "vector_magnitude_count"="ac",
    "vm"="ac", "ac"="ac", "counts"="ac", "count"="ac",
    "vector_magnitude_"="ac", "vector_magnitude__"="ac",
    "vector_magnitude_dup"="ac",
    "vector_magnitude_counts_dup"="ac",

    # axes (allowed, but not treated as "metrics" for this loader)
    "axis1"="axis1","axis2"="axis2","axis3"="axis3",
    "xis1"="axis1","xis2"="axis2","xis3"="axis3",

    # epoch (if user provides)
    "epoch_sec"="epoch_sec", "epoch"="epoch_sec", "epoch_seconds"="epoch_sec"
  )

  is_num_dest <- function(dest) {
    dest %in% c("ac","enmo","mad","mims_unit","ai","rocam","axis1","axis2","axis3","epoch_sec","age")
  }

  init_dest <- function(dest) {
    if (!(dest %in% names(DT))) {
      if (is_num_dest(dest)) DT[, (dest) := NA_real_]
      else DT[, (dest) := NA_character_]
    }
  }

  coalesce_to <- function(dest, srcs) {
    srcs <- intersect(srcs, names(DT))
    if (!length(srcs)) return(invisible(NULL))
    init_dest(dest)

    if (is_num_dest(dest)) {
      cols <- c(dest, srcs)
      DT[, (dest) := data.table::fcoalesce(
        lapply(cols, function(z) suppressWarnings(as.numeric(get(z))))
      )]
    } else {
      for (src in srcs) {
        DT[is.na(get(dest)) & !is.na(get(src)), (dest) := as.character(get(src))]
      }
    }
  }

  dests <- unique(unname(map))
  for (dest in dests) {
    srcs <- names(map)[map == dest]
    coalesce_to(dest, srcs)
  }

  # drop synonym columns (keep only canonical dests)
  drop_cols <- intersect(setdiff(names(map), dests), names(DT))
  if (length(drop_cols)) DT[, (drop_cols) := NULL]

  if (!("time" %in% names(DT))) stop("No time column detected (time/datetime/timestamp/date).")

  # -----------------------------
  # robust time parsing (base)
  # -----------------------------
  parse_time_strict <- function(x) {
    if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz = "UTC"))

    # numeric unix time (sec or ms)
    if (is.numeric(x)) {
      xx <- suppressWarnings(as.numeric(x))
      return(ifelse(xx > 1e11,
                    as.POSIXct(xx/1000, origin="1970-01-01", tz="UTC"),
                    as.POSIXct(xx,      origin="1970-01-01", tz="UTC")))
    }

    s <- trimws(as.character(x))
    s[s == ""] <- NA_character_
    out <- rep(as.POSIXct(NA), length(s))

    # ISO 8601 (Z / with T)
    iso_idx <- !is.na(s) & grepl("T", s, fixed = TRUE)
    if (any(iso_idx)) {
      tmp <- suppressWarnings(as.POSIXct(
        s[iso_idx],
        tz = "UTC",
        tryFormats = c(
          "%Y-%m-%dT%H:%M:%OSZ",
          "%Y-%m-%dT%H:%M:%OS",
          "%Y-%m-%dT%H:%M:%SZ",
          "%Y-%m-%dT%H:%M:%S"
        )
      ))
      out[iso_idx] <- tmp
    }

    rem <- is.na(out) & !is.na(s)
    if (!any(rem)) return(out)

    sr <- s[rem]
    parts <- strsplit(sr, "\\s+", perl = TRUE)
    date_part <- vapply(parts, function(z) z[1], character(1))
    time_part <- vapply(parts, function(z) if (length(z) >= 2) z[2] else "", character(1))

    d <- suppressWarnings(as.POSIXct(strptime(date_part, format="%m/%d/%Y", tz="UTC")))
    miss_d <- is.na(d)
    if (any(miss_d)) {
      d2 <- suppressWarnings(as.POSIXct(strptime(date_part[miss_d], format="%Y-%m-%d", tz="UTC")))
      d[miss_d] <- d2
    }

    hh <- mm <- ss <- integer(length(time_part))
    one_colon <- nzchar(time_part) & grepl("^[0-9]{1,2}:[0-9]{2}$", time_part, perl = TRUE)
    two_colon <- nzchar(time_part) & grepl("^[0-9]{1,2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?$", time_part, perl = TRUE)

    if (any(one_colon)) {
      tt <- strsplit(time_part[one_colon], ":", fixed = TRUE)
      hh[one_colon] <- as.integer(vapply(tt, function(z) z[1], character(1)))
      mm[one_colon] <- as.integer(vapply(tt, function(z) z[2], character(1)))
      ss[one_colon] <- 0L
    }
    if (any(two_colon)) {
      tt <- strsplit(time_part[two_colon], ":", fixed = TRUE)
      hh[two_colon] <- as.integer(vapply(tt, function(z) z[1], character(1)))
      mm[two_colon] <- as.integer(vapply(tt, function(z) z[2], character(1)))
      sec_raw <- vapply(tt, function(z) z[3], character(1))
      sec_raw <- sub("\\..*$", "", sec_raw)
      ss[two_colon] <- suppressWarnings(as.integer(sec_raw))
      ss[is.na(ss)] <- 0L
    }

    ok <- !is.na(d)
    out_rem <- rep(as.POSIXct(NA), length(sr))
    if (any(ok)) {
      add_secs <- hh[ok]*3600 + mm[ok]*60 + ss[ok]
      out_rem[ok] <- d[ok] + add_secs
    }

    out[rem] <- out_rem
    out
  }

  DT[, time := parse_time_strict(time)]
  if (all(is.na(DT$time))) stop("Failed to parse time column into POSIXct.")

  # -----------------------------
  # id
  # -----------------------------
  if (!("id" %in% names(DT))) DT[, id := "ID-unknown"]
  DT[, id := as.character(id)]

  # -----------------------------
  # normalize sex to M/F/All
  # -----------------------------
  if ("sex" %in% names(DT)) {
    DT[, sex := {
      s <- toupper(trimws(as.character(sex)))
      sn <- suppressWarnings(as.numeric(s))
      s[is.finite(sn) & sn == 1] <- "M"
      s[is.finite(sn) & sn == 2] <- "F"
      s[s %in% c("MALE","MAN","M")] <- "M"
      s[s %in% c("FEMALE","WOMAN","F")] <- "F"
      s[s %in% c("ALL","BOTH")] <- "All"
      s[!(s %in% c("M","F","All"))] <- NA_character_
      s
    }]
  }

  # age
  if ("age" %in% names(DT)) DT[, age := suppressWarnings(as.integer(round(as.numeric(age))))]

  # -----------------------------
  # ALERT user if missing age/sex (soft warning)
  # -----------------------------
  if (!("sex" %in% names(DT)) || all(is.na(DT$sex))) {
    warning(
      "No usable 'sex' column detected. Proceeding with sex='All' (sex-specific NHANES categories will not be used).",
      call. = FALSE
    )
    DT[, sex := "All"]
  }
  if (!("age" %in% names(DT)) || all(is.na(DT$age))) {
    warning(
      "No usable 'age' column detected. Proceeding without age (age-range percentiles may be limited; categories may default).",
      call. = FALSE
    )
    DT[, age := NA_integer_]
  }

  # -----------------------------
  # epoch detection:
  #   Prefer provided epoch_sec if present and non-NA,
  #   else detect from time deltas (mode).
  #   HARD STOP unless epoch_sec is exactly 5 or 60 (and not mixed).
  # -----------------------------
  detect_epoch_mode <- function(tt) {
    tt <- sort(unique(tt))
    if (length(tt) < 2L) return(NA_integer_)
    dd <- as.numeric(difftime(tt[-1], tt[-length(tt)], units = "secs"))
    dd <- dd[is.finite(dd) & dd > 0]
    if (!length(dd)) return(NA_integer_)
    tab <- table(dd)
    as.integer(names(tab)[which.max(tab)])
  }

  ep_user <- NULL
  if ("epoch_sec" %in% names(DT)) {
    ep_user <- unique(na.omit(as.integer(round(suppressWarnings(as.numeric(DT$epoch_sec))))))
    ep_user <- ep_user[is.finite(ep_user)]
    if (length(ep_user) == 0) ep_user <- NULL
  }

  if (!is.null(ep_user)) {
    if (length(ep_user) > 1) {
      stop(
        "Multiple epoch_sec values found in file: ", paste(ep_user, collapse = ", "),
        ". This system requires exactly one epoch (5 or 60)."
      )
    }
    DT[, epoch_sec := as.integer(ep_user[1])]
    epoch_source <- "provided_epoch_sec"
  } else {
    epoch_from_time <- detect_epoch_mode(DT$time)
    DT[, epoch_sec := as.integer(epoch_from_time)]
    epoch_source <- "detected_from_time"
  }

  ep_final <- unique(na.omit(as.integer(DT$epoch_sec)))
  if (!length(ep_final) || is.na(ep_final[1])) {
    stop("Could not determine epoch_sec. Provide an 'epoch_sec' column (5 or 60), or ensure time is regularly spaced.")
  }
  if (length(ep_final) > 1) {
    stop(
      "Mixed epoch_sec detected after parsing: ", paste(ep_final, collapse = ", "),
      ". This system requires exactly one epoch (5 or 60)."
    )
  }
  if (!(ep_final[1] %in% c(5L, 60L))) {
    stop(paste0(
      "Unsupported epoch_sec=", ep_final[1], ". This system ONLY supports epoch_sec 5 or 60.\n",
      "Fix: export 5s or 60s data (or recompute metrics at 5s/60s), and include epoch_sec accordingly."
    ))
  }

  # -----------------------------
  # Require >= 1 numeric metric column (single metric OK)
  # -----------------------------
  non_metric <- c("id","time","epoch_sec","sex","age","axis1","axis2","axis3")
  candidate <- setdiff(names(DT), non_metric)
  if (!length(candidate)) {
    stop("No candidate metric columns detected. Provide at least one metric column (e.g., enmo, mad, mims_unit, ac, ai, rocam).")
  }
  numeric_metrics <- candidate[vapply(DT[, ..candidate], is.numeric, logical(1))]
  if (!length(numeric_metrics)) {
    stop("No numeric metric columns detected. Provide at least one numeric metric column (e.g., enmo, mad, mims_unit, ac, ai, rocam).")
  }

  data.table::setorder(DT, id, time)

  # -----------------------------
  # Optional: valid day/week filtering
  #   - Day: calendar day in UTC
  #   - Week: 7-day blocks per-id starting at first observed day in file
  # -----------------------------
  apply_valid_day_week_filter <- function(DT, epoch_sec,
                                          valid_day_hours = 16,
                                          valid_week_days = 3) {
    ep <- as.integer(epoch_sec)
    rec_min <- ep / 60

    DT[, day_date := as.Date(time, tz = "UTC")]

    day_sum <- DT[, .(
      n_records = .N,
      wear_min  = .N * rec_min
    ), by = .(id, day_date)]
    day_sum[, valid_day := is.finite(wear_min) & wear_min >= (as.numeric(valid_day_hours) * 60)]

    day_sum[, day0 := min(day_date, na.rm = TRUE), by = id]
    day_sum[, week_id := as.integer(floor(as.numeric(day_date - day0) / 7)) + 1L]
    day_sum[, day0 := NULL]

    wk_sum <- day_sum[, .(
      n_days_week       = .N,
      n_valid_days_week = sum(valid_day, na.rm = TRUE)
    ), by = .(id, week_id)]
    wk_sum[, valid_week := n_valid_days_week >= as.integer(valid_week_days)]

    keep_keys <- merge(
      day_sum[valid_day == TRUE, .(id, day_date, week_id)],
      wk_sum[valid_week == TRUE, .(id, week_id)],
      by = c("id", "week_id"),
      all = FALSE
    )

    before <- list(
      n_rows  = nrow(DT),
      n_ids   = data.table::uniqueN(DT$id),
      n_days  = data.table::uniqueN(DT[, .(id, day_date)]),
      n_weeks = data.table::uniqueN(day_sum[, .(id, week_id)])
    )

    DT2 <- DT[keep_keys, on = .(id, day_date), nomatch = 0L]
    DT2[, day_date := NULL]

    after <- list(
      n_rows = nrow(DT2),
      n_ids  = data.table::uniqueN(DT2$id),
      n_days = data.table::uniqueN(DT2[, .(id, as.Date(time, tz="UTC"))])
    )

    meta <- list(
      valid_day_hours = as.numeric(valid_day_hours),
      valid_week_days = as.integer(valid_week_days),
      before = before,
      after  = after
    )

    list(DT = DT2, meta = meta)
  }

  valid_filter_meta <- NULL
  if (isTRUE(apply_valid_filters)) {
    res <- apply_valid_day_week_filter(
      DT,
      epoch_sec = ep_final[1],
      valid_day_hours = valid_day_hours,
      valid_week_days = valid_week_days
    )
    DT <- res$DT
    valid_filter_meta <- res$meta

    if (!nrow(DT)) {
      stop(
        "After valid-day/week filtering, 0 rows remain.\n",
        "Rules: valid day >= ", valid_day_hours, "h, valid week >= ", valid_week_days, " valid days.\n",
        "Tip: check epoch_sec, time parsing, and missingness/gaps."
      )
    }
  }

  # -----------------------------
  # attach loader metadata (for Output5)
  # -----------------------------
  loader_meta <- list(
    source_path            = path,
    source_ext             = ext,
    n_rows                 = nrow(DT),
    n_cols                 = ncol(DT),

    detected_metrics_num   = numeric_metrics,
    n_detected_metrics_num = length(numeric_metrics),

    epoch_sec              = as.integer(ep_final[1]),
    epoch_source           = epoch_source,

    has_sex                = "sex" %in% names(DT) && any(!is.na(DT$sex)),
    has_age                = "age" %in% names(DT) && any(!is.na(DT$age)),

    time_min_utc           = as.character(min(DT$time, na.rm = TRUE)),
    time_max_utc           = as.character(max(DT$time, na.rm = TRUE)),
    n_unique_id            = data.table::uniqueN(DT$id),

    # validity filter settings + results
    apply_valid_filters    = isTRUE(apply_valid_filters),
    valid_day_hours        = as.numeric(valid_day_hours),
    valid_week_days        = as.integer(valid_week_days),
    valid_filter_summary   = valid_filter_meta
  )

  attr(DT, "ua_loader_meta") <- loader_meta
  DT[]
}
