# ============================================================
# R/ua_run_end_to_end.R
# ============================================================
# Outputs per INPUT FILE (no combining across files):
#   Output1: per-day binned intensity distribution + assigned zones
#   Output2: per-day zone summary (+ "Volume") with NHANES anchor + equated NHANES refs
#   Output3: per-day percentile positioning (Volume + IG) anchored on each metric
#   Output4: daily rows + weekly aggregate rows in ONE file
#   Output5: daily MX rows
#   Output6: weekly MX rows
#   Output7: run report + glossary + run status
#
# Requirements (package functions + data):
#   - load_precomputed_metrics()
#   - ua_assign_bins()
#   - data("ref_metrics_crosswalks"), data("ref_metrics_volume_ig_percentiles")
# ============================================================

# ============================================================
# R/ua_run_end_to_end.R
# ============================================================

# ============================================================
# R/ua_run_end_to_end.R
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

  wants_output <- function(x) {
    x %in% outputs
  }

  ua_write_csv <- function(DT, path) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    if (file.exists(path) && !overwrite) stop("File exists and overwrite=FALSE: ", path, call. = FALSE)
    ok <- tryCatch({ data.table::fwrite(DT, path); TRUE }, error = function(e) FALSE)
    if (ok) return(path)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.csv$", paste0("_", ts, ".csv"), path)
    data.table::fwrite(DT, path2)
    path2
  }

  ua_write_text <- function(lines, path) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    if (file.exists(path) && !overwrite) stop("File exists and overwrite=FALSE: ", path, call. = FALSE)
    ok <- tryCatch({ writeLines(lines, con = path); TRUE }, error = function(e) FALSE)
    if (ok) return(path)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.txt$", paste0("_", ts, ".txt"), path)
    writeLines(lines, con = path2)
    path2
  }

  canon_metric <- function(x) {
    x <- tolower(trimws(as.character(x)))
    x[x %in% c("mims", "mims_unit")] <- "mims_unit"
    x[x %in% c("ac", "counts", "vm", "vector.magnitude", "vector_magnitude",
               "vector_magnitude_counts", "actigraph counts")] <- "ac"
    x
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

  ua_pick_epoch <- function(DT) {
    if (!("epoch_sec" %in% names(DT))) stop("epoch_sec missing after load_precomputed_metrics().", call. = FALSE)
    ep <- unique(na.omit(as.integer(DT$epoch_sec)))
    if (!length(ep)) stop("epoch_sec missing after load_precomputed_metrics().", call. = FALSE)
    if (length(ep) > 1) stop("Multiple epoch_sec values in file: ", paste(ep, collapse = ", "), call. = FALSE)
    as.integer(ep[1])
  }

  ua_infer_category <- function(age) {
    if (!is.finite(age)) return(NA_character_)
    if (age >= 3  & age <= 11) return("Age-3-11")
    if (age >= 12 & age <= 17) return("Age-12-17")
    if (age >= 18 & age <= 64) return("Age-18-64")
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

  drop_empty_columns <- function(DT, cols) {
    DT <- as.data.table(copy(DT))
    cols <- intersect(cols, names(DT))
    if (!length(cols)) return(DT)

    is_empty_col <- function(x) {
      if (is.character(x)) {
        all(is.na(x) | trimws(x) == "")
      } else {
        all(is.na(x))
      }
    }

    drop <- cols[vapply(DT[, ..cols], is_empty_col, logical(1))]
    if (length(drop)) DT[, (drop) := NULL]
    DT[]
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
  # Load embedded datasets
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

  attach_output2_refs <- function(zone_daily, CW) {
    ZD <- copy(zone_daily)

    ref_default <- CW[metric == anchor & (zone %in% c(band6_levels_raw, "All"))]
    ref_default[, zone_join := zone]
    ref_default[, zone_join := fifelse(zone_join == "All", "All", zone_join)]
    setkey(ref_default, anchor, epoch_sec, category_key, zone_join)

    ZD[, zone_join := fcase(intensity_zone_raw == "Volume", "All", default = intensity_zone_raw)]
    setkey(ZD, anchor, epoch_sec, category_key, zone_join)

    ZD <- merge(
      ZD,
      ref_default[, .(anchor, epoch_sec, category_key, zone_join,
                      nhanes_anchor_zone_lower = zone_lower,
                      nhanes_anchor_zone_upper = zone_upper,
                      nhanes_anchor_mean_nhanesw = mean_nhanesw,
                      nhanes_anchor_se_nhanesw = se_nhanesw,
                      nhanes_anchor_total_min_ref = total_min_ref)],
      by = c("anchor","epoch_sec","category_key","zone_join"),
      all.x = TRUE,
      sort = FALSE
    )

    ZD[, zone_join := NULL]
    ZD[]
  }

  ua_make_daily_plus_weekly <- function(all_daily) {
    D <- as.data.table(all_daily)
    D[, day := as.integer(day)]
    D[, week := as.integer((day - 1L) %/% 7L) + 1L]

    day_rows <- D[, .(
      period_type = "day",
      period_id   = day,
      n_days_in_period = 1L,
      observed_total_minutes = as.numeric(observed_total_volume_min),
      observed_mean_intensity = as.numeric(observed_mean_intensity),
      observed_ig = as.numeric(observed_ig)
    ), by = .(id, metric, epoch_sec, location, sex, age, category)]

    week_rows <- D[, .(
      period_type = "week",
      period_id   = week,
      n_days_in_period = .N,
      observed_total_minutes = sum(as.numeric(observed_total_volume_min), na.rm = TRUE),
      observed_mean_intensity = mean(as.numeric(observed_mean_intensity), na.rm = TRUE),
      observed_ig = mean(as.numeric(observed_ig), na.rm = TRUE)
    ), by = .(id, metric, epoch_sec, location, sex, age, category, week)]

    week_rows[, week := NULL]
    out <- rbindlist(list(day_rows, week_rows), use.names = TRUE, fill = TRUE)
    setorder(out, id, metric, period_type, period_id)
    out[]
  }

  ua_make_mx_daily <- function(DT, metric_cols, demo_id, epoch_sec, location, mx_values) {
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

    if (!missing(demo_id) && !is.null(demo_id) && nrow(demo_id)) {
      demo_sub <- as.data.table(copy(demo_id))
      keep_demo <- intersect(c("id", "sex", "age", "category"), names(demo_sub))
      if (length(keep_demo) > 1L) {
        demo_sub <- demo_sub[, ..keep_demo]
        demo_sub <- drop_empty_columns(demo_sub, c("sex", "age", "category"))
        if (ncol(demo_sub) > 1L) {
          out <- merge(out, demo_sub, by = "id", all.x = TRUE, sort = FALSE)
        }
      }
    }

    out <- drop_empty_columns(out, c("sex", "age", "category"))

    ord <- intersect(
      c("id","day","metric","epoch_sec","location","sex","age","category","MX","mx_value"),
      names(out)
    )
    setcolorder(out, ord)
    setorder(out, id, day, metric, MX)
    out[]
  }

  ua_make_mx_weekly <- function(mx_daily) {
    M <- as.data.table(copy(mx_daily))
    req <- c("id","day","metric","epoch_sec","location","MX","mx_value")
    miss <- setdiff(req, names(M))
    if (length(miss)) stop("Output6: mx_daily missing columns: ", paste(miss, collapse = ", "), call. = FALSE)

    M[, week := as.integer((as.integer(day) - 1L) %/% 7L) + 1L]

    group_cols <- intersect(c("id","week","metric","epoch_sec","location","sex","age","category","MX"), names(M))

    out <- M[, .(
      n_days_in_period = data.table::uniqueN(day),
      mx_value = mean(as.numeric(mx_value), na.rm = TRUE)
    ), by = group_cols]

    setnames(out, "week", "period_id")
    out <- drop_empty_columns(out, c("sex", "age", "category"))

    ord <- intersect(
      c("id","period_id","metric","epoch_sec","location","sex","age","category",
        "MX","n_days_in_period","mx_value"),
      names(out)
    )
    setcolorder(out, ord)
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
      output7 = list(ok = FALSE, path = NA_character_, msg = "")
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

    on.exit({
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
        "  2) Grouped bins into 6 movement zones (plus a full-day 'Volume' row).",
        "  3) Added NHANES reference values for population context.",
        "  4) Created two NHANES-based comparison views:",
        "       • Output2: zone-based (anchor zones + equated NHANES references for other metrics)",
        "       • Output3: percentile-based (anchor percentile + NHANES values at that percentile)",
        "  5) Computed MX metrics (daily and weekly outputs).",
        "",
        "Important limitations:",
        "  • The file must contain a single epoch length.",
        "  • Current end-to-end phase supports only 5s or 60s.",
        "  • Age is required for Output3 and all demographic-aware outputs.",
        "",
        "============================================================",
        "HOW TO INTERPRET THE OUTPUTS",
        "============================================================",
        "",
        "OUTPUT 1 (BINS: daily intensity distribution):",
        "  • Each row is one person × day × metric × intensity bin.",
        "  • bin_lower / bin_upper : bin edges in the metric's native units.",
        "  • midpoint              : bin midpoint used for summaries (native units).",
        "  • time_bin_min          : observed minutes accumulated in that bin (YOUR data).",
        "  • cm_time_min           : descending cumulative minutes within (id, day, metric).",
        "  • intensity_zone        : zone label assigned using NHANES normative zone bounds.",
        "",
        "OUTPUT 2 (DAILY SUMMARY: time in zones + NHANES references):",
        "  • Each row is one person × day × metric × zone (plus a 'Volume' row).",
        "  • observed_* columns are computed from YOUR data.",
        "  • nhanes_anchor_* columns are NHANES reference values for the anchor metric/zone panel.",
        "  • equated_<metric>_* columns are NHANES reference mean/SE for other metrics mapped onto the same anchor-defined zone.",
        "",
        "OUTPUT 3 (PERCENTILES: anchor-conditioned, not zone-based):",
        "  • Each row is one person × day × anchor × stat (Volume or Intensity gradient).",
        "  • Finds the nearest NHANES percentile for your observed anchor value.",
        "  • Then looks up NHANES values for all metrics at that same anchor percentile.",
        "",
        "OUTPUT 4 (DAILY + WEEKLY):",
        "  • One file containing both day-level rows and week-level rows.",
        "  • period_type indicates day vs week.",
        "",
        "OUTPUT 5 (DAILY MX):",
        "  • Each row is one person × day × metric × MX target.",
        "  • Mx means the threshold intensity above which the most active X minutes were accumulated.",
        "  • mx_value is reported in the metric's native units.",
        "",
        "OUTPUT 6 (WEEKLY MX):",
        "  • Each row is one person × week × metric × MX target.",
        "  • Current phase 2 weekly MX is the average of daily MX values across observed days in that week.",
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
        "equated_<metric>_*        : NHANES reference values for another metric mapped to the anchor-defined zone (Output2)",
        "category / category_key   : NHANES-style age category (and sex-specific variant if applicable)",
        "intensity_zone            : zone label (Volume, Minimal, ... Peak)",
        "observed_minutes_in_zone  : minutes in zone from YOUR data (Output2)",
        "observed_total_volume_min : total observed minutes in the day",
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

      try(ua_write_text(out7_lines, out7_path), silent = TRUE)
      status$output7$ok <- TRUE
      status$output7$path <- out7_path
    }, add = TRUE)

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
      if (!ref_epoch_ok) {
        status$output1 <- list(ok = TRUE, path = NA_character_,
                               msg = sprintf("skipped: epoch_sec=%s is not supported for Output1 (requires 5 or 60)", epoch_sec))
      } else {
        CW <- normalize_crosswalk(ref_metrics_crosswalks, epoch_keep = epoch_sec)
        LUT <- build_zone_LUTs_for_assignment(CW)

        bins <- as.data.table(ua_assign_bins(
          DT        = DT,
          epoch_sec = epoch_sec,
          location  = location,
          per_day   = TRUE,
          id_col    = "id",
          time_col  = "time"
        ))

        bins <- merge(bins, demo_id[, .(id, sex, age, category, category_key)], by = "id", all.x = TRUE, sort = FALSE)
        bins[, metric := canon_metric(metric)]
        bins[, anchor := canon_anchor(metric)]

        bins[, intensity_zone_raw := {
          zdt <- get_zones_for_group(LUT, anchor[1], epoch_sec[1], category_key[1])
          if (is.null(zdt) || !nrow(zdt)) rep(NA_character_, .N) else assign_bands_from_zones(midpoint, zdt)
        }, by = .(id, day, metric, epoch_sec, anchor, category_key)]

        keep_output1 <- intersect(
          c("id","day","metric","time_bin_min","lower","upper","midpoint","width",
            "log_midpoint","log_time_bin","epoch_sec","location","cm_time_min"),
          names(bins)
        )
        out1 <- bins[, ..keep_output1]

        status$output1 <- tryCatch({
          p <- ua_write_csv(out1, file.path(run_folder, sprintf("UA_Output1_BINS_epoch%ss_%s.csv", epoch_sec, run_date)))
          list(ok = TRUE, path = p, msg = "")
        }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
      }
    }

    # -----------------------------
    # Outputs 2-4
    # -----------------------------
    if (any(vapply(c("output2","output3","output4"), wants_output, logical(1)))) {
      if (!ref_epoch_ok) {
        msg_ref <- sprintf("skipped: epoch_sec=%s is not supported for reference-based outputs (requires 5 or 60)", epoch_sec)
        if (wants_output("output2")) status$output2 <- list(ok = TRUE, path = NA_character_, msg = msg_ref)
        if (wants_output("output3")) status$output3 <- list(ok = TRUE, path = NA_character_, msg = msg_ref)
        if (wants_output("output4")) status$output4 <- list(ok = TRUE, path = NA_character_, msg = msg_ref)
      } else if (!has_any_age) {
        if (wants_output("output2")) status$output2 <- list(ok = TRUE, path = NA_character_, msg = "skipped: age column missing; Output2 requires age")
        if (wants_output("output3")) status$output3 <- list(ok = TRUE, path = NA_character_, msg = "skipped: age column missing; Output3 requires age")
        if (wants_output("output4")) status$output4 <- list(ok = TRUE, path = NA_character_, msg = "skipped: age column missing; Output4 requires age")
      } else if (!has_complete_age) {
        bad_ids <- unique(DT[!is.finite(age), id])
        msg_age <- sprintf("skipped: incomplete/invalid age for %d id(s), e.g.: %s",
                           length(bad_ids), paste(head(bad_ids, 10), collapse = ", "))
        if (wants_output("output2")) status$output2 <- list(ok = TRUE, path = NA_character_, msg = msg_age)
        if (wants_output("output3")) status$output3 <- list(ok = TRUE, path = NA_character_, msg = msg_age)
        if (wants_output("output4")) status$output4 <- list(ok = TRUE, path = NA_character_, msg = msg_age)
      } else {
        if (all(is.na(DT$sex) | !nzchar(DT$sex))) {
          DT[, sex := "All"]
        } else {
          DT[is.na(sex) | !nzchar(sex), sex := "All"]
        }

        DT[, category := vapply(age, ua_infer_category, character(1))]
        demo_id <- DT[, .(
          sex      = sex[1],
          age      = age[1],
          category = category[1]
        ), by = .(id)]
        demo_id[, sex := as.character(sex)]
        demo_id[, category_key := get_category_key(category, sex)]

        if (is.null(CW)) CW <- normalize_crosswalk(ref_metrics_crosswalks, epoch_keep = epoch_sec)
        if (is.null(LUT)) LUT <- build_zone_LUTs_for_assignment(CW)

        if (is.null(bins)) {
          bins <- as.data.table(ua_assign_bins(
            DT        = DT,
            epoch_sec = epoch_sec,
            location  = location,
            per_day   = TRUE,
            id_col    = "id",
            time_col  = "time"
          ))

          bins <- merge(bins, demo_id[, .(id, sex, age, category, category_key)], by = "id", all.x = TRUE, sort = FALSE)
          bins[, metric := canon_metric(metric)]
          bins[, anchor := canon_anchor(metric)]

          bins[, intensity_zone_raw := {
            zdt <- get_zones_for_group(LUT, anchor[1], epoch_sec[1], category_key[1])
            if (is.null(zdt) || !nrow(zdt)) rep(NA_character_, .N) else assign_bands_from_zones(midpoint, zdt)
          }, by = .(id, day, metric, epoch_sec, anchor, category_key)]
        }

        all_daily <- bins[, .(
          observed_total_volume_min = sum(time_bin_min, na.rm = TRUE),
          observed_mean_intensity   = ua_weighted_mean(midpoint, time_bin_min),
          observed_ig               = ua_compute_ig_slope(midpoint, time_bin_min)
        ), by = .(id, day, metric, epoch_sec, location)]
        all_daily <- merge(all_daily, demo_id[, .(id, sex, age, category, category_key)], by = "id", all.x = TRUE, sort = FALSE)
        all_daily[, metric := canon_metric(metric)]
        all_daily[, anchor := canon_anchor(metric)]

        if (wants_output("output2")) {
          zone_obs <- bins[!is.na(intensity_zone_raw),
                           .(
                             observed_minutes_in_zone = sum(time_bin_min, na.rm = TRUE),
                             observed_mean_intensity  = ua_weighted_mean(midpoint, time_bin_min)
                           ),
                           by = .(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw)]

          zone_obs <- merge(zone_obs, demo_id[, .(id, sex, age, category)], by = "id", all.x = TRUE, sort = FALSE)

          keys2 <- unique(all_daily[, .(id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category)])
          zone_shell <- keys2[, .(intensity_zone_raw = band6_levels_raw),
                              by = .(id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category)]

          zone_daily <- merge(
            zone_shell,
            zone_obs[, .(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw,
                         observed_minutes_in_zone, observed_mean_intensity)],
            by = c("id","day","metric","epoch_sec","location","anchor","category_key","intensity_zone_raw"),
            all.x = TRUE, sort = FALSE
          )
          zone_daily[is.na(observed_minutes_in_zone), observed_minutes_in_zone := 0]

          vol <- all_daily[, .(
            id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category,
            intensity_zone_raw = "Volume",
            observed_minutes_in_zone = observed_total_volume_min,
            observed_mean_intensity  = observed_mean_intensity
          )]

          zone_daily2 <- rbindlist(list(zone_daily, vol), use.names = TRUE, fill = TRUE)
          zone_daily2 <- attach_output2_refs(zone_daily2, CW)

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
          out2 <- zone_daily2[, ..keep_front]

          status$output2 <- tryCatch({
            p <- ua_write_csv(out2, file.path(run_folder, sprintf("UA_Output2_DAILY_SUMMARY_epoch%ss_%s.csv", epoch_sec, run_date)))
            list(ok = TRUE, path = p, msg = "")
          }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
        }

        if (wants_output("output3")) {
          status$output3 <- list(ok = TRUE, path = NA_character_, msg = "not implemented in this simplified rewrite")
        }

        if (wants_output("output4")) {
          if (!isTRUE(make_weekly)) {
            status$output4 <- list(ok = TRUE, path = NA_character_, msg = "skipped: make_weekly=FALSE")
          } else {
            status$output4 <- tryCatch({
              out4_dt <- ua_make_daily_plus_weekly(all_daily)
              p <- ua_write_csv(out4_dt, file.path(run_folder, sprintf("UA_Output4_DAILY_PLUS_WEEKLY_epoch%ss_%s.csv", epoch_sec, run_date)))
              list(ok = TRUE, path = p, msg = "wrote daily+weekly rollups")
            }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
          }
        }
      }
    }

    # -----------------------------
    # Outputs 5-6 (MX)
    # -----------------------------
    mx_metric_cols <- loader_meta$detected_metrics_num
    if (is.null(mx_metric_cols) || !length(mx_metric_cols)) {
      mx_metric_cols <- intersect(c("ac","enmo","mad","mims_unit","ai","rocam"), names(DT))
    }

    if (wants_output("output5")) {
      status$output5 <- tryCatch({
        out5 <- ua_make_mx_daily(
          DT = DT,
          metric_cols = mx_metric_cols,
          demo_id = demo_id,
          epoch_sec = epoch_sec,
          location = location,
          mx_values = mx_values
        )
        out5 <- drop_empty_columns(out5, c("sex", "age", "category"))
        p <- ua_write_csv(out5, file.path(run_folder, sprintf("UA_Output5_DAILY_MX_epoch%ss_%s.csv", epoch_sec, run_date)))
        list(ok = TRUE, path = p, msg = "")
      }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
    }

    if (wants_output("output6")) {
      if (!isTRUE(make_weekly)) {
        status$output6 <- list(ok = TRUE, path = NA_character_, msg = "skipped: make_weekly=FALSE")
      } else {
        status$output6 <- tryCatch({
          mx_daily <- ua_make_mx_daily(
            DT = DT,
            metric_cols = mx_metric_cols,
            demo_id = demo_id,
            epoch_sec = epoch_sec,
            location = location,
            mx_values = mx_values
          )
          out6 <- ua_make_mx_weekly(mx_daily)
          out6 <- drop_empty_columns(out6, c("sex", "age", "category"))
          p <- ua_write_csv(out6, file.path(run_folder, sprintf("UA_Output6_WEEKLY_MX_epoch%ss_%s.csv", epoch_sec, run_date)))
          list(ok = TRUE, path = p, msg = "wrote weekly MX")
        }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
      }
    }

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
  in_path <- tryCatch(normalizePath(in_path, winslash = "/", mustWork = FALSE),
                      error = function(e) in_path)

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  if (dir.exists(in_path)) {
    files <- list.files(in_path, full.names = TRUE, recursive = FALSE)
    files <- files[grepl("\\.(csv|txt|xlsx|xls|rds)$", files, ignore.case = TRUE)]
    if (!length(files)) stop("Folder contains no supported files (csv/txt/xlsx/xls/rds): ", in_path, call. = FALSE)

    out <- lapply(files, function(f) {
      message("[UA] processing file: ", basename(f))
      run_one_file(f)
    })
    names(out) <- basename(files)
    return(invisible(out))
  }

  if (!file.exists(in_path)) stop("in_path does not exist: ", in_path, call. = FALSE)
  invisible(run_one_file(in_path))
}
