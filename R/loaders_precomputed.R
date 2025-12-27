#' Load precomputed metric time-series for binning / IG (robust messy inputs)
#'
#' Canonical output columns:
#'   id, time (POSIXct), epoch_sec (5 or 60), sex (M/F/All), age (int),
#'   plus numeric metric cols (any of: ac,enmo,mad,mims_unit,ai,rocam, ...)
#'   plus: day (dense index per-id based on day_tz)
#'
#' HARD RULES:
#'   - epoch_sec must be exactly 5 or 60 (single value; not mixed)
#'   - if apply_valid_filters=TRUE then valid day is NON-NEGOTIABLE:
#'       keep ONLY id×day_date with >= valid_day_hours hours of UNIQUE epoch timestamps
#'       (NO WEEK FILTER)
#'
#' @param path File path (.csv/.txt/.xlsx/.xls/.rds)
#' @param apply_valid_filters logical; apply strict valid-day filtering
#' @param valid_day_hours numeric; default 16
#' @param valid_week_days integer; DEPRECATED/ignored (kept for backward compatibility)
#' @param day_tz character; timezone used to define "day" boundaries.
#'   - Default "UTC" (reproducible)
#'   - Use "" to use the system local timezone
#'   - Or provide an IANA tz like "America/Chicago"
#' @export
load_precomputed_metrics <- function(path,
                                     apply_valid_filters = TRUE,
                                     valid_day_hours = 16,
                                     valid_week_days = 3,   # ignored
                                     day_tz = "UTC") {

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

    # axes (allowed, not required)
    "axis1"="axis1","axis2"="axis2","axis3"="axis3",
    "xis1"="axis1","xis2"="axis2","xis3"="axis3",

    # epoch (if user provides)
    "epoch_sec"="epoch_sec", "epoch"="epoch_sec", "epoch_seconds"="epoch_sec"
  )

  is_num_dest <- function(dest) {
    dest %in% c("ac","enmo","mad","mims_unit","ai","rocam",
                "axis1","axis2","axis3","epoch_sec","age")
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

  # SAFE: drop only synonym columns that exist, and are not canonical dests
  syn_cols_present <- intersect(names(map), names(DT))
  drop_cols <- setdiff(syn_cols_present, dests)
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

    # ISO 8601
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
  if ("age" %in% names(DT)) DT[, age := suppressWarnings(as.integer(round(as.numeric(age))))]

  if (!("sex" %in% names(DT)) || all(is.na(DT$sex))) {
    warning("No usable 'sex' column detected. Proceeding with sex='All'.", call. = FALSE)
    DT[, sex := "All"]
  }
  if (!("age" %in% names(DT)) || all(is.na(DT$age))) {
    warning("No usable 'age' column detected. Proceeding with age=NA.", call. = FALSE)
    DT[, age := NA_integer_]
  }

  # -----------------------------
  # epoch detection (5 or 60 only)
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
    if (length(ep_user) > 1) stop("Multiple epoch_sec values found: ", paste(ep_user, collapse = ", "))
    DT[, epoch_sec := as.integer(ep_user[1])]
    epoch_source <- "provided_epoch_sec"
  } else {
    DT[, epoch_sec := as.integer(detect_epoch_mode(time))]
    epoch_source <- "detected_from_time"
  }

  ep_final <- unique(na.omit(as.integer(DT$epoch_sec)))
  if (!length(ep_final) || is.na(ep_final[1])) stop("Could not determine epoch_sec.")
  if (length(ep_final) > 1) stop("Mixed epoch_sec detected: ", paste(ep_final, collapse = ", "))
  if (!(ep_final[1] %in% c(5L, 60L))) stop("Unsupported epoch_sec=", ep_final[1], " (only 5 or 60)")

  data.table::setorder(DT, id, time)

  # -----------------------------
  # Strict VALID DAY filtering ONLY (NO WEEK FILTER)
  #   - uses UNIQUE snapped epoch timestamps per id × day_date (day_tz)
  #   - NEVER trusts any incoming 'day' column; we recompute later
  # -----------------------------
  apply_valid_day_filter <- function(DT, epoch_sec, valid_day_hours = 16, day_tz = "UTC") {
    ep <- as.integer(epoch_sec)
    rec_min <- ep / 60

    tnum <- as.numeric(DT$time)
    DT[, time_epoch := as.POSIXct(floor(tnum / ep) * ep, origin="1970-01-01", tz="UTC")]

    # day boundary determined by day_tz ("" => system tz)
    DT[, day_date := as.Date(time_epoch, tz = day_tz)]

    day_sum <- DT[, .(
      n_rows_raw   = .N,
      n_epoch_uniq = data.table::uniqueN(time_epoch),
      wear_min     = data.table::uniqueN(time_epoch) * rec_min
    ), by=.(id, day_date)]
    day_sum[, valid_day := is.finite(wear_min) & wear_min >= (as.numeric(valid_day_hours) * 60)]

    keep <- day_sum[valid_day == TRUE, .(id, day_date)]
    DT2 <- DT[keep, on=.(id, day_date), nomatch = 0L]

    # Recompute canonical day index per id from day_date (dense)
    DT2[, day := data.table::frank(day_date, ties.method="dense"), by=id]

    DT2[, c("time_epoch","day_date") := NULL]

    meta <- list(
      day_tz = day_tz,
      valid_day_hours = as.numeric(valid_day_hours),
      before = list(
        n_rows = nrow(DT),
        n_ids  = data.table::uniqueN(DT$id),
        n_days = data.table::uniqueN(day_sum[, .(id, day_date)])
      ),
      after = list(
        n_rows = nrow(DT2),
        n_ids  = data.table::uniqueN(DT2$id),
        n_days = data.table::uniqueN(DT2[, .(id, day)])
      )
    )

    list(DT=DT2, meta=meta)
  }

  valid_filter_meta <- NULL
  if (isTRUE(apply_valid_filters)) {
    res <- apply_valid_day_filter(
      DT,
      epoch_sec = ep_final[1],
      valid_day_hours = valid_day_hours,
      day_tz = day_tz
    )
    DT <- res$DT
    valid_filter_meta <- res$meta

    if (!nrow(DT)) stop("After valid-day filtering, 0 rows remain.")

    # HARD ASSERT: no sub-<valid_day_hours> days remain (using same day_tz definition)
    ep <- as.integer(ep_final[1])
    tnum <- as.numeric(DT$time)
    DT[, time_epoch := as.POSIXct(floor(tnum / ep) * ep, origin="1970-01-01", tz="UTC")]
    DT[, day_date := as.Date(time_epoch, tz = day_tz)]

    chk <- DT[, .(
      wear_min = data.table::uniqueN(time_epoch) * (ep/60)
    ), by=.(id, day_date)]

    bad <- chk[wear_min < as.numeric(valid_day_hours)*60]
    if (nrow(bad)) {
      print(bad[order(wear_min)][1:min(.N, 25)])
      stop("VALID-DAY ASSERT FAILED: sub-", valid_day_hours, "h day(s) remain after filtering.")
    }

    DT[, c("time_epoch","day_date") := NULL]
  } else {
    # Even if no filtering, still ensure a canonical day exists for downstream
    ep <- as.integer(ep_final[1])
    tnum <- as.numeric(DT$time)
    DT[, time_epoch := as.POSIXct(floor(tnum / ep) * ep, origin="1970-01-01", tz="UTC")]
    DT[, day_date := as.Date(time_epoch, tz = day_tz)]
    DT[, day := data.table::frank(day_date, ties.method="dense"), by=id]
    DT[, c("time_epoch","day_date") := NULL]
  }

  # -----------------------------
  # Require >= 1 numeric metric column (single metric OK)
  # -----------------------------
  non_metric <- c("id","time","epoch_sec","sex","age","day","axis1","axis2","axis3")
  candidate <- setdiff(names(DT), non_metric)
  numeric_metrics <- candidate[vapply(DT[, ..candidate], is.numeric, logical(1))]
  if (!length(numeric_metrics)) stop("No numeric metric columns detected.")

  # -----------------------------
  # attach loader metadata
  # -----------------------------
  loader_meta <- list(
    source_path = path,
    source_ext  = ext,
    n_rows      = nrow(DT),
    n_cols      = ncol(DT),

    detected_metrics_num   = numeric_metrics,
    n_detected_metrics_num = length(numeric_metrics),

    epoch_sec    = as.integer(ep_final[1]),
    epoch_source = epoch_source,

    day_tz       = as.character(day_tz),

    time_min     = as.character(min(DT$time, na.rm=TRUE)),
    time_max     = as.character(max(DT$time, na.rm=TRUE)),
    n_unique_id  = data.table::uniqueN(DT$id),

    apply_valid_filters  = isTRUE(apply_valid_filters),
    valid_day_hours      = as.numeric(valid_day_hours),
    valid_filter_summary = valid_filter_meta,

    valid_week_days_arg_ignored = as.integer(valid_week_days)
  )

  attr(DT, "ua_loader_meta") <- loader_meta
  DT[]
}
