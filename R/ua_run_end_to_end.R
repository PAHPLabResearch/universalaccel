# ============================================================
# R/ua_run_end_to_end.R
# ============================================================
# Outputs per INPUT FILE (no combining across files):
#   Output1: per-day binned intensity distribution
#   Output2: IG & Volume (daily rows + weekly aggregate rows in one file)
#   Output3: daily MX rows
#   Output4: weekly MX rows
#   Output5: daily zone summary using NHANES anchor zones + equated NHANES references
#   Output6: IG & Volume percentiles in NHANES
#   Output7: run report + glossary + run status
#
# Demographic policy in this revision:
#   - Only Output5 and Output6 are demographic-aware / NHANES-referenced
#   - Output2, Output3, Output4 do NOT carry age/sex/category columns
#
# Requirements (package functions + data):
#   - load_precomputed_metrics()
#   - ua_assign_bins()
#   - data("ref_metrics_crosswalks"), data("ref_metrics_volume_ig_percentiles")
# ============================================================

ua_run_end_to_end <- function(in_path,
                              out_dir,
                              location = "ndw",
                              make_weekly = TRUE,
                              mx_values = c(1, 2, 5, 10, 15, 20, 30, 45, 60, 120, 240, 360, 480, 600, 720),
                              overwrite = TRUE,
                              outputs = c("output1","output2","output3","output4","output5","output6")) {

  stopifnot(requireNamespace("data.table", quietly = TRUE))
  library(data.table)

  # ---------------------------
  # Helpers
  # ---------------------------
  safe_slug <- function(x) {
    x <- as.character(x)
    x <- gsub("\\.[A-Za-z0-9]+$", "", x)
    x <- gsub("[^A-Za-z0-9_-]+", "_", x)
    x <- gsub("_+", "_", x)
    x <- gsub("^_|_$", "", x)
    if (!nzchar(x)) x <- "input"
    x
  }

  wants_output <- function(x) x %in% outputs

  ua_write_csv <- function(DT, path) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    if (file.exists(path) && !overwrite) {
      stop("File exists and overwrite=FALSE: ", path, call. = FALSE)
    }

    ok <- tryCatch({
      data.table::fwrite(DT, path)
      TRUE
    }, error = function(e) FALSE)

    if (ok) return(path)

    ts2 <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.csv$", paste0("_", ts2, ".csv"), path)
    data.table::fwrite(DT, path2)
    path2
  }

  ua_write_text <- function(lines, path) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    if (file.exists(path) && !overwrite) {
      stop("File exists and overwrite=FALSE: ", path, call. = FALSE)
    }

    ok <- tryCatch({
      writeLines(lines, con = path)
      TRUE
    }, error = function(e) FALSE)

    if (ok) return(path)

    ts2 <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.txt$", paste0("_", ts2, ".txt"), path)
    writeLines(lines, con = path2)
    path2
  }

  canon_metric <- function(x) {
    x <- toupper(trimws(as.character(x)))
    x[x %in% c("MIMS", "MIMS_UNIT")] <- "MIMS_UNIT"
    x[x %in% c("AC", "COUNTS", "VM", "VECTOR_MAGNITUDE", "VECTOR_MAGNITUDE_COUNTS", "ACTIGRAPH COUNTS")] <- "AC"
    tolower(x)
  }

  canon_anchor <- function(x) canon_metric(x)

  canon_sex <- function(x) {
    s <- tolower(trimws(as.character(x)))
    s[s %in% c("", "na", "n/a", "unknown")] <- NA_character_

    sn <- suppressWarnings(as.numeric(s))
    s[is.finite(sn) & sn == 1] <- "m"
    s[is.finite(sn) & sn == 2] <- "f"

    s[s %in% c("male", "man", "m")] <- "M"
    s[s %in% c("female", "woman", "f")] <- "F"
    s[s %in% c("all", "both", "pooled", "overall")] <- "All"

    s[!(s %in% c("M","F","All"))] <- NA_character_
    s
  }

  canon_sex_name_ref <- function(x) {
    s <- trimws(as.character(x))
    out <- rep(NA_character_, length(s))
    out[tolower(s) %in% c("male", "m")] <- "M"
    out[tolower(s) %in% c("female", "f")] <- "F"
    out[tolower(s) %in% c("all", "both", "pooled", "overall")] <- "All"
    out
  }

  canon_stat_ref <- function(x) {
    s <- trimws(as.character(x))
    out <- s
    out[tolower(s) %in% c("average acceleration")] <- "Average acceleration"
    out[tolower(s) %in% c("intensity gradient")]   <- "Intensity gradient"
    out
  }

  parse_percentile_chr <- function(x) {
    s <- trimws(as.character(x))
    suppressWarnings(as.numeric(gsub("[^0-9.]+", "", s)))
  }

  ua_pick_epoch <- function(DT) {
    if (!("epoch_sec" %in% names(DT))) {
      stop("epoch_sec missing after load_precomputed_metrics().", call. = FALSE)
    }
    ep <- unique(na.omit(as.integer(DT$epoch_sec)))
    if (!length(ep)) {
      stop("epoch_sec missing after load_precomputed_metrics().", call. = FALSE)
    }
    if (length(ep) > 1) {
      stop("Multiple epoch_sec values in file: ", paste(ep, collapse = ", "), call. = FALSE)
    }
    as.integer(ep[1])
  }

  ua_infer_category <- function(age) {
    if (!is.finite(age)) return(NA_character_)
    if (age >= 3  && age <= 11) return("Age-3-11")
    if (age >= 12 && age <= 17) return("Age-12-17")
    if (age >= 18 && age <= 64) return("Age-18-64")
    if (age >= 65) return("Age-65+")
    NA_character_
  }

  get_category_key <- function(category_base, sex) {
    sx <- canon_sex(sex)
    ifelse(sx %in% c("M","F"),
           paste0(as.character(category_base), "_Sex-", sx),
           as.character(category_base))
  }

  ua_weighted_mean <- function(x, w) {
    ok <- is.finite(x) & is.finite(w) & w > 0
    if (!any(ok)) return(NA_real_)
    sum(x[ok] * w[ok]) / sum(w[ok])
  }

  ua_compute_ig_slope <- function(midpoint, minutes) {
    x <- suppressWarnings(as.numeric(midpoint))
    y <- suppressWarnings(as.numeric(minutes))
    ok <- is.finite(x) & is.finite(y) & x > 0 & y > 0
    if (sum(ok) < 3) return(NA_real_)
    fit <- stats::lm(log(y[ok]) ~ log(x[ok]))
    b <- coef(fit)[["log(x[ok])"]]
    if (!is.finite(b)) return(NA_real_)
    unname(b)
  }

  ua_compute_mx_group <- function(values, epoch_sec, mx_values) {
    vals <- suppressWarnings(as.numeric(values))
    vals <- vals[is.finite(vals) & vals >= 0]
    if (!length(vals)) return(NULL)

    vals <- sort(vals, decreasing = TRUE)
    n_avail <- length(vals)

    out <- lapply(mx_values, function(mx) {
      n_epochs <- ceiling((as.numeric(mx) * 60) / as.numeric(epoch_sec))
      n_take <- min(n_epochs, n_avail)
      if (n_take <= 0) return(NULL)

      data.table(
        MX = paste0("M", mx),
        mx_value = min(vals[seq_len(n_take)], na.rm = TRUE)
      )
    })

    rbindlist(out, use.names = TRUE, fill = TRUE)
  }

  band6_levels_raw <- c(
    "Minimal intensity zone",
    "Low-to-Moderate intensity zone",
    "Moderate-to-High intensity zone",
    "High-to-Very High intensity zone",
    "Very High-to-Peak intensity zone",
    "Peak intensity zone"
  )

  strip_intensity_zone_phrase <- function(z) {
    z <- as.character(z)
    z[z == "All"] <- "Volume"
    z <- gsub(" intensity zone$", "", z, ignore.case = TRUE)
    z
  }

  # ---------------------------
  # Embedded datasets
  # ---------------------------
  if (!exists("ref_metrics_crosswalks", inherits = TRUE)) {
    utils::data("ref_metrics_crosswalks", package = "universalaccel", envir = environment())
  }
  if (!exists("ref_metrics_volume_ig_percentiles", inherits = TRUE)) {
    utils::data("ref_metrics_volume_ig_percentiles", package = "universalaccel", envir = environment())
  }

  normalize_crosswalk <- function(crosswalk_dt, epoch_keep) {
    CW0 <- as.data.table(crosswalk_dt)
    setnames(CW0, tolower(names(CW0)))

    CW <- CW0[, .(
      anchor        = canon_anchor(anchor),
      epoch_sec     = as.integer(epoch_sec),
      category_key  = as.character(category),
      metric        = canon_metric(metric),
      zone          = as.character(zone),
      zone_lower    = suppressWarnings(as.numeric(zone_lower)),
      zone_upper    = suppressWarnings(as.numeric(zone_upper)),
      mean_nhanesw  = suppressWarnings(as.numeric(mean_nhanesw)),
      se_nhanesw    = suppressWarnings(as.numeric(se_nhanesw)),
      total_min_ref = suppressWarnings(as.numeric(total_min))
    )]

    CW[epoch_sec == as.integer(epoch_keep)]
  }

  normalize_percentiles <- function(percentile_dt, epoch_keep) {
    P0 <- as.data.table(percentile_dt)
    setnames(P0, tolower(names(P0)))

    req <- c("epoch_sec","anchor","metric","sex_name","stat","perc","age","value")
    miss <- setdiff(req, names(P0))
    if (length(miss)) {
      stop("Percentile reference missing required column(s): ", paste(miss, collapse = ", "), call. = FALSE)
    }

    P <- P0[, .(
      epoch_sec = as.integer(epoch_sec),
      anchor    = canon_anchor(anchor),
      metric    = canon_metric(metric),
      sex       = canon_sex_name_ref(sex_name),
      stat      = canon_stat_ref(stat),
      percentile = parse_percentile_chr(perc),
      age       = suppressWarnings(as.integer(age)),
      value     = suppressWarnings(as.numeric(value))
    )]

    P <- P[
      epoch_sec == as.integer(epoch_keep) &
        stat %in% c("Average acceleration", "Intensity gradient") &
        is.finite(percentile) &
        is.finite(age) &
        !is.na(sex) &
        !is.na(value) &
        nzchar(anchor) &
        nzchar(metric)
    ]

    P[, se_nhanesw := NA_real_]
    setorder(P, anchor, epoch_sec, sex, age, stat, metric, percentile)
    P[]
  }

  build_zone_LUTs_for_assignment <- function(CW) {
    Z <- CW[zone %in% band6_levels_raw & metric == anchor]
    Z <- Z[is.finite(zone_lower) & is.finite(zone_upper)]
    setorder(Z, anchor, epoch_sec, category_key, zone)
    Z <- Z[, .SD[1L], by = .(anchor, epoch_sec, category_key, zone)]

    Z[, key := paste0(anchor, "|", epoch_sec, "|", category_key)]
    keys <- unique(Z$key)

    LUT <- setNames(vector("list", length(keys)), keys)
    for (k in keys) {
      zz <- Z[key == k, .(
        band            = zone,
        intensity_lower = zone_lower,
        intensity_upper = zone_upper
      )]
      zz <- zz[match(band6_levels_raw, zz$band)]
      LUT[[k]] <- zz
    }
    LUT
  }

  get_zones_for_group <- function(LUT, anchor, epoch_sec, category_key) {
    LUT[[paste0(anchor, "|", epoch_sec, "|", category_key)]]
  }

  assign_bands_from_zones <- function(x, zones_dt) {
    x <- suppressWarnings(as.numeric(x))
    out <- rep(NA_character_, length(x))
    if (is.null(zones_dt) || !nrow(zones_dt)) return(out)

    rng <- range(c(zones_dt$intensity_lower, zones_dt$intensity_upper), finite = TRUE)
    scale <- max(1, abs(rng[2] - rng[1]))
    tol <- 1e-10 * scale

    zmin <- zones_dt[zones_dt$band == "Minimal intensity zone", ]
    if (nrow(zmin)) {
      lo <- zmin$intensity_lower[1]
      if (is.finite(lo)) out[is.finite(x) & abs(x - lo) <= tol] <- "Minimal intensity zone"
    }

    for (b in band6_levels_raw[band6_levels_raw != "Minimal intensity zone"]) {
      zz <- zones_dt[zones_dt$band == b, ]
      if (!nrow(zz)) next
      lo <- zz$intensity_lower[1]
      hi <- zz$intensity_upper[1]
      idx <- which(is.na(out) & is.finite(x) & (x > lo + tol) & (x <= hi + tol))
      if (length(idx)) out[idx] <- b
    }
    out
  }

  attach_output5_refs <- function(zone_daily, CW) {
    ZD <- copy(zone_daily)

    ref_anchor <- CW[metric == anchor & (zone %in% c(band6_levels_raw, "All"))]
    ref_anchor[, zone_join := fifelse(zone == "All", "All", zone)]

    ZD[, zone_join := fifelse(intensity_zone_raw == "Volume", "All", intensity_zone_raw)]

    ZD <- merge(
      ZD,
      ref_anchor[, .(
        anchor, epoch_sec, category_key, zone_join,
        nhanes_anchor_zone_lower = zone_lower,
        nhanes_anchor_zone_upper = zone_upper,
        nhanes_anchor_mean_nhanesw = mean_nhanesw,
        nhanes_anchor_se_nhanesw = se_nhanesw,
        nhanes_anchor_total_min_ref = total_min_ref
      )],
      by = c("anchor","epoch_sec","category_key","zone_join"),
      all.x = TRUE,
      sort = FALSE
    )

    ref_equated <- CW[zone %in% c(band6_levels_raw, "All")]
    ref_equated[, zone_join := fifelse(zone == "All", "All", zone)]

    eq_mean <- dcast(
      ref_equated,
      anchor + epoch_sec + category_key + zone_join ~ metric,
      value.var = "mean_nhanesw"
    )
    mean_cols <- setdiff(names(eq_mean), c("anchor","epoch_sec","category_key","zone_join"))
    if (length(mean_cols)) {
      setnames(eq_mean, mean_cols, paste0("equated_", mean_cols, "_mean_nhanesw"))
    }

    eq_se <- dcast(
      ref_equated,
      anchor + epoch_sec + category_key + zone_join ~ metric,
      value.var = "se_nhanesw"
    )
    se_cols <- setdiff(names(eq_se), c("anchor","epoch_sec","category_key","zone_join"))
    if (length(se_cols)) {
      setnames(eq_se, se_cols, paste0("equated_", se_cols, "_se_nhanesw"))
    }

    ZD <- merge(
      ZD, eq_mean,
      by = c("anchor","epoch_sec","category_key","zone_join"),
      all.x = TRUE, sort = FALSE
    )
    ZD <- merge(
      ZD, eq_se,
      by = c("anchor","epoch_sec","category_key","zone_join"),
      all.x = TRUE, sort = FALSE
    )

    ZD[, zone_join := NULL]
    ZD[]
  }

  build_output6_percentiles <- function(all_daily, PCT) {
    AD <- as.data.table(copy(all_daily))
    P  <- as.data.table(copy(PCT))

    obs_pct <- rbindlist(list(
      AD[, .(
        id, day, epoch_sec, location,
        metric, anchor, sex, age, category, category_key,
        stat = "Average acceleration",
        observed_value = observed_mean_intensity
      )],
      AD[, .(
        id, day, epoch_sec, location,
        metric, anchor, sex, age, category, category_key,
        stat = "Intensity gradient",
        observed_value = observed_ig
      )]
    ), use.names = TRUE, fill = TRUE)

    obs_pct <- obs_pct[is.finite(observed_value) & is.finite(age)]
    if (!nrow(obs_pct)) {
      stop("No finite observed Average acceleration / Intensity gradient values available for Output6.", call. = FALSE)
    }

    # Find nearest percentile row for the anchor metric, exact age, preferred sex first then All fallback
    pick_one <- function(anchor_i, epoch_i, sex_i, age_i, stat_i, obs_i) {
      sex_try <- if (!is.na(sex_i) && sex_i %in% c("M","F")) c(sex_i, "All") else "All"

      ref <- P[
        anchor == anchor_i &
          epoch_sec == epoch_i &
          age == age_i &
          stat == stat_i &
          metric == anchor_i &
          sex %in% sex_try
      ]

      if (!nrow(ref)) {
        return(data.table(
          percentile = NA_real_,
          ref_sex = NA_character_,
          nhanes_anchor_value = NA_real_,
          nhanes_anchor_se_nhanesw = NA_real_
        ))
      }

      ref[, sex_pref := match(sex, sex_try)]
      ref[, abs_diff := abs(value - obs_i)]
      setorder(ref, sex_pref, abs_diff, percentile)
      best <- ref[1]

      data.table(
        percentile = best$percentile,
        ref_sex = best$sex,
        nhanes_anchor_value = best$value,
        nhanes_anchor_se_nhanesw = best$se_nhanesw
      )
    }

    out6_anchor <- rbindlist(
      lapply(seq_len(nrow(obs_pct)), function(i) {
        r <- obs_pct[i]
        cbind(
          r,
          pick_one(
            anchor_i = r$anchor,
            epoch_i  = r$epoch_sec,
            sex_i    = r$sex,
            age_i    = r$age,
            stat_i   = r$stat,
            obs_i    = r$observed_value
          )
        )
      }),
      use.names = TRUE,
      fill = TRUE
    )

    # bring in all metrics at matched percentile for same anchor/age/sex/stat
    ref_wide_val <- dcast(
      P,
      anchor + epoch_sec + sex + age + stat + percentile ~ metric,
      value.var = "value"
    )
    val_cols <- setdiff(names(ref_wide_val), c("anchor","epoch_sec","sex","age","stat","percentile"))
    if (length(val_cols)) {
      setnames(ref_wide_val, val_cols, paste0("nhanes_", val_cols))
    }

    out6 <- merge(
      out6_anchor,
      ref_wide_val,
      by.x = c("anchor","epoch_sec","ref_sex","age","stat","percentile"),
      by.y = c("anchor","epoch_sec","sex","age","stat","percentile"),
      all.x = TRUE,
      sort = FALSE
    )

    keep_front <- c(
      "id","day","epoch_sec","location",
      "metric","anchor",
      "sex","age","category","category_key",
      "stat","observed_value","percentile","ref_sex",
      "nhanes_anchor_value","nhanes_anchor_se_nhanesw"
    )

    other_cols <- setdiff(names(out6), keep_front)
    setcolorder(out6, c(intersect(keep_front, names(out6)), other_cols))
    setorder(out6, id, day, anchor, stat)

    out6[]
  }

  ua_make_ig_volume_daily_weekly <- function(all_daily) {
    D <- as.data.table(copy(all_daily))
    D[, day := as.integer(day)]
    D[, week := as.integer((day - 1L) %/% 7L) + 1L]

    day_rows <- D[, .(
      period_type = "day",
      period_id   = day,
      n_days_in_period = 1L,
      observed_total_volume_min = as.numeric(observed_total_volume_min),
      observed_mean_intensity   = as.numeric(observed_mean_intensity),
      observed_ig               = as.numeric(observed_ig)
    ), by = .(id, metric, epoch_sec, location)]

    week_rows <- D[, .(
      period_type = "week",
      period_id   = week,
      n_days_in_period = .N,
      observed_total_volume_min = sum(as.numeric(observed_total_volume_min), na.rm = TRUE),
      observed_mean_intensity   = mean(as.numeric(observed_mean_intensity), na.rm = TRUE),
      observed_ig               = mean(as.numeric(observed_ig), na.rm = TRUE)
    ), by = .(id, metric, epoch_sec, location, week)]

    week_rows[, week := NULL]
    out <- rbindlist(list(day_rows, week_rows), use.names = TRUE, fill = TRUE)
    setorder(out, id, metric, period_type, period_id)
    out[]
  }

  ua_make_mx_daily <- function(DT, metric_cols, epoch_sec, location, mx_values) {
    X <- as.data.table(copy(DT))
    keep_metrics <- intersect(metric_cols, names(X))
    keep_metrics <- setdiff(keep_metrics, c("axis1","axis2","axis3","epoch_sec","age"))
    if (!length(keep_metrics)) stop("No usable metric columns available for MX.", call. = FALSE)

    long <- melt(
      X,
      id.vars = c("id", "day"),
      measure.vars = keep_metrics,
      variable.name = "metric",
      value.name = "value",
      variable.factor = FALSE
    )

    long[, metric := canon_metric(metric)]
    long <- long[is.finite(value) & value >= 0]
    if (!nrow(long)) stop("No finite non-negative values available for MX after reshaping.", call. = FALSE)

    out <- long[, {
      mx_dt <- ua_compute_mx_group(value, epoch_sec = epoch_sec[1], mx_values = mx_values)
      if (is.null(mx_dt)) NULL else mx_dt
    }, by = .(id, day, metric)]

    out[, epoch_sec := as.integer(epoch_sec)]
    out[, location := location]

    ord <- c("id","day","metric","epoch_sec","location","MX","mx_value")
    setcolorder(out, intersect(ord, names(out)))
    setorder(out, id, day, metric, MX)
    out[]
  }

  ua_make_mx_weekly <- function(mx_daily) {
    M <- as.data.table(copy(mx_daily))
    req <- c("id","day","metric","epoch_sec","location","MX","mx_value")
    miss <- setdiff(req, names(M))
    if (length(miss)) {
      stop("Output4: mx_daily missing columns: ", paste(miss, collapse = ", "), call. = FALSE)
    }

    M[, week := as.integer((as.integer(day) - 1L) %/% 7L) + 1L]

    out <- M[, .(
      n_days_in_period = data.table::uniqueN(day),
      mx_value = mean(as.numeric(mx_value), na.rm = TRUE)
    ), by = .(id, week, metric, epoch_sec, location, MX)]

    setnames(out, "week", "period_id")

    ord <- c("id","period_id","metric","epoch_sec","location","MX","n_days_in_period","mx_value")
    setcolorder(out, intersect(ord, names(out)))
    setorder(out, id, period_id, metric, MX)
    out[]
  }

  # ============================================================
  # Run ONE FILE
  # ============================================================
  run_one_file <- function(file_path) {
    dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
    run_date <- format(Sys.Date(), "%Y-%m-%d")
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")

    status <- list(
      output1 = list(ok = FALSE, path = NA_character_, msg = "not requested"),
      output2 = list(ok = FALSE, path = NA_character_, msg = "not requested"),
      output3 = list(ok = FALSE, path = NA_character_, msg = "not requested"),
      output4 = list(ok = FALSE, path = NA_character_, msg = "not requested"),
      output5 = list(ok = FALSE, path = NA_character_, msg = "not requested"),
      output6 = list(ok = FALSE, path = NA_character_, msg = "not requested"),
      output7 = list(ok = FALSE, path = NA_character_, msg = "not requested")
    )

    DT <- as.data.table(load_precomputed_metrics(
      file_path,
      apply_valid_filters = TRUE,
      valid_day_hours = 16,
      valid_week_days = 0
    ))

    loader_meta <- attr(DT, "ua_loader_meta")
    if (!is.list(loader_meta)) loader_meta <- list()

    if (!("id" %in% names(DT))) stop("No 'id' column after load_precomputed_metrics().", call. = FALSE)
    if (!("time" %in% names(DT))) stop("No 'time' column after load_precomputed_metrics().", call. = FALSE)

    if (!("age" %in% names(DT))) DT[, age := NA_integer_]
    DT[, age := suppressWarnings(as.integer(round(as.numeric(age))))]

    if (!("sex" %in% names(DT))) DT[, sex := NA_character_]
    DT[, sex := canon_sex(sex)]
    DT[, sex := as.character(sex)]
    DT[!nzchar(sex), sex := NA_character_]

    epoch_sec <- ua_pick_epoch(DT)
    ref_epoch_ok <- epoch_sec %in% c(5L, 60L)

    has_any_age <- any(is.finite(DT$age))
    has_complete_age <- all(is.finite(DT$age))

    input_slug <- safe_slug(basename(file_path))
    run_root <- file.path(out_dir, "UA_runs")
    run_folder <- file.path(run_root, sprintf("%s_epoch%ss_%s", input_slug, epoch_sec, ts))
    dir.create(run_folder, recursive = TRUE, showWarnings = FALSE)

    DT[, category := vapply(age, ua_infer_category, character(1))]

    demo_id <- DT[, .(
      sex      = sex[which(!is.na(sex) & nzchar(sex))[1]],
      age      = age[which(is.finite(age))[1]],
      category = category[which(!is.na(category) & nzchar(category))[1]]
    ), by = .(id)]

    demo_id[, sex := as.character(sex)]
    demo_id[, category_key := ifelse(
      !is.na(category) & nzchar(category) & !is.na(sex) & nzchar(sex),
      get_category_key(category, sex),
      NA_character_
    )]

    bins <- NULL
    all_daily <- NULL
    CW <- NULL
    LUT <- NULL

    # -----------------------------
    # Output1
    # -----------------------------
    if (wants_output("output1")) {
      status$output1 <- tryCatch({
        bins1 <- as.data.table(ua_assign_bins(
          DT        = DT,
          epoch_sec = epoch_sec,
          location  = location,
          per_day   = TRUE,
          id_col    = "id",
          time_col  = "time"
        ))

        bins1[, metric := canon_metric(metric)]

        keep_output1 <- intersect(
          c("id","day","metric","time_bin_min","lower","upper","midpoint","width",
            "log_midpoint","log_time_bin","epoch_sec","location","cm_time_min"),
          names(bins1)
        )
        out1 <- bins1[, ..keep_output1]

        p <- ua_write_csv(
          out1,
          file.path(run_folder, sprintf("UA_Output1_BINS_epoch%ss_%s.csv", epoch_sec, run_date))
        )
        list(ok = TRUE, path = p, msg = "")
      }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
    }

    # -----------------------------
    # Shared all_daily block for Output2 and Output5/6
    # -----------------------------
    need_all_daily <- any(vapply(c("output2","output5","output6"), wants_output, logical(1)))

    if (need_all_daily) {
      if (!ref_epoch_ok) {
        msg_ref <- sprintf("skipped: epoch_sec=%s is not supported for NHANES/reference-based outputs (requires 5 or 60)", epoch_sec)
        if (wants_output("output2")) status$output2 <- list(ok = TRUE, path = NA_character_, msg = msg_ref)
        if (wants_output("output5")) status$output5 <- list(ok = TRUE, path = NA_character_, msg = msg_ref)
        if (wants_output("output6")) status$output6 <- list(ok = TRUE, path = NA_character_, msg = msg_ref)
      } else {
        DT_ref <- copy(DT)

        if (all(is.na(DT_ref$sex) | !nzchar(DT_ref$sex))) {
          DT_ref[, sex := "All"]
        } else {
          DT_ref[is.na(sex) | !nzchar(sex), sex := "All"]
        }

        DT_ref[, category := vapply(age, ua_infer_category, character(1))]
        demo_id_ref <- DT_ref[, .(
          sex      = sex[1],
          age      = age[1],
          category = category[1]
        ), by = .(id)]
        demo_id_ref[, sex := as.character(sex)]
        demo_id_ref[, category_key := get_category_key(category, sex)]

        if (is.null(CW)) CW <- normalize_crosswalk(ref_metrics_crosswalks, epoch_keep = epoch_sec)
        if (is.null(LUT)) LUT <- build_zone_LUTs_for_assignment(CW)

        bins <- as.data.table(ua_assign_bins(
          DT        = DT_ref,
          epoch_sec = epoch_sec,
          location  = location,
          per_day   = TRUE,
          id_col    = "id",
          time_col  = "time"
        ))

        bins <- merge(
          bins,
          demo_id_ref[, .(id, sex, age, category, category_key)],
          by = "id",
          all.x = TRUE,
          sort = FALSE
        )

        bins[, metric := canon_metric(metric)]
        bins[, anchor := canon_anchor(metric)]

        bins[, intensity_zone_raw := {
          zdt <- get_zones_for_group(LUT, anchor[1], epoch_sec[1], category_key[1])
          if (is.null(zdt) || !nrow(zdt)) rep(NA_character_, .N) else assign_bands_from_zones(midpoint, zdt)
        }, by = .(id, day, metric, epoch_sec, anchor, category_key)]

        all_daily <- bins[, .(
          observed_total_volume_min = sum(time_bin_min, na.rm = TRUE),
          observed_mean_intensity   = ua_weighted_mean(midpoint, time_bin_min),
          observed_ig               = ua_compute_ig_slope(midpoint, time_bin_min)
        ), by = .(id, day, metric, epoch_sec, location)]

        all_daily <- merge(
          all_daily,
          demo_id_ref[, .(id, sex, age, category, category_key)],
          by = "id",
          all.x = TRUE,
          sort = FALSE
        )

        all_daily[, metric := canon_metric(metric)]
        all_daily[, anchor := canon_anchor(metric)]
      }
    }

    # -----------------------------
    # Output2: IG & Volume
    # -----------------------------
    if (wants_output("output2")) {
      if (ref_epoch_ok) {
        status$output2 <- tryCatch({
          out2_dt <- ua_make_ig_volume_daily_weekly(all_daily)
          p <- ua_write_csv(
            out2_dt,
            file.path(run_folder, sprintf("UA_Output2_IG_AND_VOLUME_epoch%ss_%s.csv", epoch_sec, run_date))
          )
          list(ok = TRUE, path = p, msg = "wrote daily+weekly IG & Volume")
        }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
      }
    }

    # -----------------------------
    # Output5: Daily zone summary using NHANES anchor zones + equated NHANES references
    # -----------------------------
    if (wants_output("output5")) {
      if (!ref_epoch_ok) {
        # already handled
      } else if (!has_any_age) {
        status$output5 <- list(ok = TRUE, path = NA_character_, msg = "skipped: age column missing; Output5 requires age")
      } else if (!has_complete_age) {
        bad_ids <- unique(DT[!is.finite(age), id])
        msg_age <- sprintf(
          "skipped: incomplete/invalid age for %d id(s), e.g.: %s",
          length(bad_ids), paste(head(bad_ids, 10), collapse = ", ")
        )
        status$output5 <- list(ok = TRUE, path = NA_character_, msg = msg_age)
      } else {
        status$output5 <- tryCatch({

          zone_obs <- bins[!is.na(intensity_zone_raw),
                           .(
                             observed_minutes_in_zone = sum(time_bin_min, na.rm = TRUE),
                             observed_mean_intensity  = ua_weighted_mean(midpoint, time_bin_min)
                           ),
                           by = .(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw)]

          zone_obs <- merge(
            zone_obs,
            all_daily[, .(id, day, metric, epoch_sec, location, sex, age, category, category_key)],
            by = c("id","day","metric","epoch_sec","location","category_key"),
            all.x = TRUE,
            sort = FALSE
          )

          keys5 <- unique(all_daily[, .(
            id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category
          )])

          zone_shell <- keys5[, .(
            intensity_zone_raw = c(band6_levels_raw, "Volume")
          ), by = .(
            id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category
          )]

          zone_daily <- merge(
            zone_shell,
            zone_obs,
            by = c("id","day","metric","epoch_sec","location","anchor",
                   "category_key","sex","age","category","intensity_zone_raw"),
            all.x = TRUE,
            sort = FALSE
          )

          zone_daily[is.na(observed_minutes_in_zone), observed_minutes_in_zone := 0]

          vol_map <- all_daily[, .(
            id, day, metric, epoch_sec, location,
            vol_minutes = observed_total_volume_min,
            vol_mean_intensity = observed_mean_intensity
          )]

          zone_daily <- merge(
            zone_daily,
            vol_map,
            by = c("id","day","metric","epoch_sec","location"),
            all.x = TRUE,
            sort = FALSE
          )

          zone_daily[intensity_zone_raw == "Volume", observed_minutes_in_zone := vol_minutes]
          zone_daily[intensity_zone_raw == "Volume", observed_mean_intensity  := vol_mean_intensity]
          zone_daily[, c("vol_minutes","vol_mean_intensity") := NULL]

          zone_daily2 <- attach_output5_refs(zone_daily, CW)
          zone_daily2[, intensity_zone := strip_intensity_zone_phrase(intensity_zone_raw)]
          zone_daily2[, intensity_zone_raw := NULL]

          keep_front <- c(
            "id","day","epoch_sec","location",
            "metric","anchor",
            "sex","age","category","category_key",
            "intensity_zone",
            "observed_minutes_in_zone","observed_mean_intensity",
            "nhanes_anchor_zone_lower","nhanes_anchor_zone_upper",
            "nhanes_anchor_mean_nhanesw","nhanes_anchor_se_nhanesw","nhanes_anchor_total_min_ref"
          )

          eq_cols <- grep("^equated_.*_(mean_nhanesw|se_nhanesw)$", names(zone_daily2), value = TRUE)

          cols5 <- c(keep_front, eq_cols)
          cols5 <- intersect(cols5, names(zone_daily2))
          out5 <- zone_daily2[, ..cols5]

          p <- ua_write_csv(
            out5,
            file.path(run_folder, sprintf("UA_Output5_DAILY_ZONE_SUMMARY_NHANES_epoch%ss_%s.csv", epoch_sec, run_date))
          )
          list(ok = TRUE, path = p, msg = "")
        }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
      }
    }

    # -----------------------------
    # Output6: IG & Volume percentiles in NHANES
    # -----------------------------
    if (wants_output("output6")) {
      if (!ref_epoch_ok) {
        # already handled
      } else if (!has_any_age) {
        status$output6 <- list(ok = TRUE, path = NA_character_, msg = "skipped: age column missing; Output6 requires age")
      } else if (!has_complete_age) {
        bad_ids <- unique(DT[!is.finite(age), id])
        msg_age <- sprintf(
          "skipped: incomplete/invalid age for %d id(s), e.g.: %s",
          length(bad_ids), paste(head(bad_ids, 10), collapse = ", ")
        )
        status$output6 <- list(ok = TRUE, path = NA_character_, msg = msg_age)
      } else {
        status$output6 <- tryCatch({
          PCT_local <- normalize_percentiles(ref_metrics_volume_ig_percentiles, epoch_keep = epoch_sec)

          if (!nrow(PCT_local)) {
            stop("No percentile reference rows available for this epoch.", call. = FALSE)
          }

          out6 <- build_output6_percentiles(all_daily, PCT_local)

          p <- ua_write_csv(
            out6,
            file.path(run_folder, sprintf("UA_Output6_IG_AND_VOLUME_PERCENTILES_NHANES_epoch%ss_%s.csv", epoch_sec, run_date))
          )
          list(ok = TRUE, path = p, msg = "")
        }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
      }
    }

    # -----------------------------
    # Outputs 3-4 (MX)
    # -----------------------------
    mx_metric_cols <- loader_meta$detected_metrics_num
    if (is.null(mx_metric_cols) || !length(mx_metric_cols)) {
      mx_metric_cols <- intersect(c("ac","enmo","mad","mims_unit","ai","rocam"), names(DT))
    }

    if (wants_output("output3")) {
      status$output3 <- tryCatch({
        out3 <- ua_make_mx_daily(
          DT = DT,
          metric_cols = mx_metric_cols,
          epoch_sec = epoch_sec,
          location = location,
          mx_values = mx_values
        )
        p <- ua_write_csv(
          out3,
          file.path(run_folder, sprintf("UA_Output3_DAILY_MX_epoch%ss_%s.csv", epoch_sec, run_date))
        )
        list(ok = TRUE, path = p, msg = "")
      }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
    }

    if (wants_output("output4")) {
      if (!isTRUE(make_weekly)) {
        status$output4 <- list(ok = TRUE, path = NA_character_, msg = "skipped: make_weekly=FALSE")
      } else {
        status$output4 <- tryCatch({
          mx_daily <- ua_make_mx_daily(
            DT = DT,
            metric_cols = mx_metric_cols,
            epoch_sec = epoch_sec,
            location = location,
            mx_values = mx_values
          )
          out4 <- ua_make_mx_weekly(mx_daily)
          p <- ua_write_csv(
            out4,
            file.path(run_folder, sprintf("UA_Output4_WEEKLY_MX_epoch%ss_%s.csv", epoch_sec, run_date))
          )
          list(ok = TRUE, path = p, msg = "wrote weekly MX")
        }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
      }
    }

    # -----------------------------
    # Output7: run report + glossary + status
    # -----------------------------
    status$output7 <- tryCatch({
      `%||%` <- function(x, y) if (is.null(x)) y else x
      metrics_line <- paste(loader_meta$detected_metrics_num %||% character(0), collapse = ", ")
      if (!nzchar(metrics_line)) metrics_line <- "(none detected)"

      out7_path <- file.path(run_folder, sprintf("UA_Output7_LOG_epoch%ss_%s.txt", epoch_sec, run_date))

      out7_lines <- c(
        "universalaccel — end-to-end run report",
        paste0("Run date: ", run_date),
        paste0("Input file: ", file_path),
        paste0("Output folder (this run): ", run_folder),
        paste0("Epoch length: ", epoch_sec, " seconds"),
        paste0("Location: ", location),
        paste0("MX values requested (minutes): ", paste(mx_values, collapse = ", ")),
        "",
        "============================================================",
        "WHAT YOU PROVIDED",
        "============================================================",
        paste0("Rows / Columns: ", (loader_meta$n_rows %||% nrow(DT)), " / ", (loader_meta$n_cols %||% ncol(DT))),
        paste0("Participants (unique IDs): ", (loader_meta$n_unique_id %||% uniqueN(DT$id))),
        paste0("Time span (UTC): ", (loader_meta$time_min_utc %||% NA_character_), " to ", (loader_meta$time_max_utc %||% NA_character_)),
        "",
        "Metrics detected in your file:",
        paste0("  • ", metrics_line),
        "",
        "============================================================",
        "WHAT THIS TOOL DID",
        "============================================================",
        "  1) Binned each day into intensity levels (minutes per bin).",
        "  2) Computed daily and weekly IG & Volume summaries.",
        "  3) Computed MX metrics (daily and weekly outputs).",
        "  4) Added NHANES reference values for daily zone-summary output.",
        "  5) Added percentile-based NHANES comparison output for average acceleration and intensity gradient.",
        "",
        "Important limitations:",
        "  • The file must contain a single epoch length.",
        "  • Current NHANES-referenced outputs support only 5s or 60s.",
        "  • Only Output5 and Output6 are demographic-aware.",
        "",
        "============================================================",
        "HOW TO INTERPRET THE OUTPUTS",
        "============================================================",
        "",
        "OUTPUT 1 (BINS: daily intensity distribution):",
        "  • Each row is one person × day × metric × intensity bin.",
        "  • lower / upper         : bin edges in the metric's native units.",
        "  • midpoint              : bin midpoint used for summaries (native units).",
        "  • time_bin_min          : observed minutes accumulated in that bin (YOUR data).",
        "  • cm_time_min           : descending cumulative minutes within (id, day, metric).",
        "  • No zone labels are included in Output1.",
        "",
        "OUTPUT 2 (IG & VOLUME):",
        "  • One file containing day-level rows and week-level rows.",
        "  • observed_total_volume_min : total minutes observed in the period.",
        "  • observed_mean_intensity   : time-weighted mean intensity in native units.",
        "  • observed_ig               : intensity gradient derived from the observed distribution.",
        "  • No age/sex/category columns are carried here.",
        "",
        "OUTPUT 3 (DAILY MX):",
        "  • Each row is one person × day × metric × MX target.",
        "  • Mx means the threshold intensity above which the most active X minutes were accumulated.",
        "  • mx_value is reported in the metric's native units.",
        "",
        "OUTPUT 4 (WEEKLY MX):",
        "  • Each row is one person × week × metric × MX target.",
        "  • Current weekly MX is the average of daily MX values across observed days in that week.",
        "",
        "OUTPUT 5 (DAILY ZONE SUMMARY + NHANES REFERENCES):",
        "  • Each row is one person × day × metric × zone (plus a Volume row).",
        "  • NHANES anchor zones are used to summarize the observed person-day data.",
        "  • observed_minutes_in_zone and observed_mean_intensity come from YOUR data.",
        "  • nhanes_anchor_* columns describe the NHANES anchor reference for that same zone.",
        "  • equated_<metric>_* columns give NHANES reference values for other metrics mapped to that same anchor-defined zone.",
        "  • This output is demographic-aware and requires age.",
        "",
        "OUTPUT 6 (IG & VOLUME PERCENTILES IN NHANES):",
        "  • Each row is one person × day × anchor-stat combination.",
        "  • 'Average acceleration' uses observed_mean_intensity.",
        "  • 'Intensity gradient' uses observed_ig.",
        "  • The observed value is matched to the nearest NHANES percentile within the same age/sex reference group.",
        "  • nhanes_anchor_value is the NHANES anchor value at that matched percentile.",
        "  • nhanes_<metric> columns show what that same percentile looks like in the other metrics.",
        "  • This output is demographic-aware and requires age.",
        "",
        "============================================================",
        "HOW TO READ KEY TERMS / SUFFIXES",
        "============================================================",
        "metric                    : metric being summarized (ac, enmo, mad, mims_unit, ...)",
        "anchor                    : anchor metric defining the NHANES lookup panel",
        "observed_*                : computed from YOUR file (your data)",
        "nhanes_*                  : NHANES reference values (population context)",
        "nhanesw                   : NHANES survey-weighted estimate",
        "mean_nhanesw              : survey-weighted mean (NHANES reference)",
        "se_nhanesw                : standard error of the survey-weighted mean (NHANES reference)",
        "equated_<metric>_*        : NHANES reference values for another metric mapped to the anchor-defined zone",
        "category / category_key   : NHANES-style age category (and sex-specific variant if applicable)",
        "intensity_zone            : zone label (Volume, Minimal, ... Peak)",
        "observed_minutes_in_zone  : minutes in zone from YOUR data (Output5)",
        "observed_total_volume_min : total observed minutes in the day/period",
        "observed_mean_intensity   : time-weighted mean intensity in native units",
        "observed_ig               : intensity gradient slope computed from your daily distribution",
        "MX / Mx                   : threshold exceeded for the most active X minutes",
        "mx_value                  : MX value in the metric's native units",
        "",
        "============================================================",
        "RUN STATUS (DEBUG)",
        "============================================================",
        sprintf("Output1 ok=%s  %s  %s", status$output1$ok, status$output1$path, status$output1$msg),
        sprintf("Output2 ok=%s  %s  %s", status$output2$ok, status$output2$path, status$output2$msg),
        sprintf("Output3 ok=%s  %s  %s", status$output3$ok, status$output3$path, status$output3$msg),
        sprintf("Output4 ok=%s  %s  %s", status$output4$ok, status$output4$path, status$output4$msg),
        sprintf("Output5 ok=%s  %s  %s", status$output5$ok, status$output5$path, status$output5$msg),
        sprintf("Output6 ok=%s  %s  %s", status$output6$ok, status$output6$path, status$output6$msg),
        "",
        "============================================================",
        "END",
        "============================================================"
      )

      p <- ua_write_text(out7_lines, out7_path)
      list(ok = TRUE, path = p, msg = "")
    }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))

    invisible(list(
      run_folder = run_folder,
      status = status
    ))
  }

  # ============================================================
  # Entry point
  # ============================================================
  if (!is.character(in_path) || length(in_path) != 1L || !nzchar(in_path)) {
    stop("in_path must be a single non-empty file or folder path.", call. = FALSE)
  }

  in_path <- tryCatch(
    normalizePath(in_path, winslash = "/", mustWork = FALSE),
    error = function(e) in_path
  )

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  if (dir.exists(in_path)) {
    files <- list.files(in_path, full.names = TRUE, recursive = FALSE)
    files <- files[grepl("\\.(csv|txt|xlsx|xls|rds)$", files, ignore.case = TRUE)]
    if (!length(files)) {
      stop("Folder contains no supported files (csv/txt/xlsx/xls/rds): ", in_path, call. = FALSE)
    }

    out <- lapply(files, function(f) {
      message("[UA] processing file: ", basename(f))
      run_one_file(f)
    })
    names(out) <- basename(files)
    return(invisible(out))
  }

  if (!file.exists(in_path)) {
    stop("in_path does not exist: ", in_path, call. = FALSE)
  }

  invisible(run_one_file(in_path))
}
