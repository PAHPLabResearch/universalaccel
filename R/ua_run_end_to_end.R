# ============================================================
# R/ua_run_end_to_end.R  (PRO OUTPUTS: Output2 + Output3 EQUATED)
# ============================================================
# What this does (high level):
#   Output1: per-day binned distribution (bins + assigned intensity zones)
#   Output2: per-day intensity-zone summary (NO IG/day-total columns) + NHANES refs
#            + EQUATED (cross-metric) NHANES mean/se on the same anchor
#   Output3: per-day AA + IG percentile positioning:
#            for each anchor+stat, compute the ANCHOR’s nearest centile,
#            then EQUATE other metrics by looking up their NHANES values at
#            that SAME anchor-centile (shared-centile mapping; anchor-conditioned).
#   Output4: optional weekly rollups (basic means)
#   Output5: plain-language run report + glossary (non-technical friendly)
#
# Requires universalaccel:
#   - load_precomputed_metrics()
#   - ua_assign_bins()
#   - datasets: bin_edges, ref_metrics_crosswalks, ref_metrics_volume_ig_percentiles
# ============================================================

ua_run_end_to_end <- function(in_path,
                              out_dir,
                              location = "ndw",
                              make_weekly = TRUE,
                              overwrite = TRUE) {

  stopifnot(requireNamespace("data.table", quietly = TRUE))
  library(data.table)

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  run_date <- format(Sys.Date(), "%Y-%m-%d")
  # ---------------------------
  # Per-input-file output folder
  # ---------------------------
  safe_slug <- function(x) {
    x <- as.character(x)
    x <- gsub("\\.[A-Za-z0-9]+$", "", x)        # drop extension
    x <- gsub("[^A-Za-z0-9_-]+", "_", x)        # replace unsafe chars
    x <- gsub("_+", "_", x)                    # collapse repeats
    x <- gsub("^_|_$", "", x)                  # trim underscores
    if (!nzchar(x)) x <- "input"
    x
  }

  input_base <- basename(in_path)
  run_folder <- file.path(out_dir, paste0(safe_slug(input_base), "_UA_outputs"))
  dir.create(run_folder, recursive = TRUE, showWarnings = FALSE)

  # ---------------------------
  # Load embedded datasets
  # ---------------------------
  if (!exists("bin_edges", inherits = TRUE)) {
    utils::data("bin_edges", package = "universalaccel", envir = environment())
  }
  if (!exists("ref_metrics_crosswalks", inherits = TRUE)) {
    utils::data("ref_metrics_crosswalks", package = "universalaccel", envir = environment())
  }
  if (!exists("ref_metrics_volume_ig_percentiles", inherits = TRUE)) {
    utils::data("ref_metrics_volume_ig_percentiles", package = "universalaccel", envir = environment())
  }
  if (!exists("bin_edges", inherits = TRUE)) stop("bin_edges not found.")
  if (!exists("ref_metrics_crosswalks", inherits = TRUE)) stop("ref_metrics_crosswalks not found.")
  if (!exists("ref_metrics_volume_ig_percentiles", inherits = TRUE)) stop("ref_metrics_volume_ig_percentiles not found.")

  # ---------------------------
  # IO helpers
  # ---------------------------
  ua_write_csv <- function(DT, path) {
    if (file.exists(path) && !overwrite) stop("File exists and overwrite=FALSE: ", path)
    ok <- tryCatch({ fwrite(DT, path); TRUE }, error = function(e) FALSE)
    if (ok) return(path)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.csv$", paste0("_", ts, ".csv"), path)
    fwrite(DT, path2)
    path2
  }

  ua_write_text <- function(lines, path) {
    if (file.exists(path) && !overwrite) stop("File exists and overwrite=FALSE: ", path)
    ok <- tryCatch({ writeLines(lines, con = path); TRUE }, error = function(e) FALSE)
    if (ok) return(path)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.txt$", paste0("_", ts, ".txt"), path)
    writeLines(lines, con = path2)
    path2
  }

  # ---------------------------
  # Canonical codes
  # ---------------------------
  canon_metric <- function(x) {
    x <- tolower(trimws(as.character(x)))
    x[x %in% c("mims", "mims_unit")] <- "mims_unit"
    x[x %in% c("ac", "counts", "vm", "vector.magnitude", "vector_magnitude",
               "vector_magnitude_counts", "actigraph counts")] <- "ac"
    x
  }
  canon_anchor <- function(x) canon_metric(x)

  canon_sex <- function(x) {
    sx <- toupper(trimws(as.character(x)))
    sx[sx %in% c("MALE","MAN")] <- "M"
    sx[sx %in% c("FEMALE","WOMAN")] <- "F"
    sx[is.na(sx) | !nzchar(sx)] <- "All"
    sx
  }

  ua_stop_epoch <- function(epoch_sec) {
    epoch_sec <- as.integer(epoch_sec)
    if (!(epoch_sec %in% c(5L, 60L))) stop("Only epoch_sec 5 or 60 supported. Got: ", epoch_sec)
    epoch_sec
  }

  ua_pick_epoch <- function(DT) {
    if (!("epoch_sec" %in% names(DT))) stop("epoch_sec missing after load_precomputed_metrics().")
    ep <- unique(na.omit(as.integer(DT$epoch_sec)))
    if (!length(ep)) stop("epoch_sec missing after load_precomputed_metrics().")
    if (length(ep) > 1) stop("Multiple epoch_sec values in file: ", paste(ep, collapse = ", "))
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

  ua_overall_intensity <- function(midpoint, time_bin_min) {
    ok <- is.finite(midpoint) & is.finite(time_bin_min) & time_bin_min > 0
    if (!any(ok)) return(NA_real_)
    sum(midpoint[ok] * time_bin_min[ok]) / sum(time_bin_min[ok])
  }

  ua_compute_ig_slope <- function(midpoint, time_bin_min) {
    # IG uses: log(time in bin) vs log(intensity bin midpoint)
    # We must avoid eps-clamping artifacts -> require strictly positive values.
    x <- suppressWarnings(as.numeric(midpoint))
    y <- suppressWarnings(as.numeric(time_bin_min))

    ok <- is.finite(x) & is.finite(y) & x > 0 & y > 0
    if (sum(ok) < 3) return(NA_real_)

    lx <- log(x[ok])
    ly <- log(y[ok])

    fit <- stats::lm(ly ~ lx)
    b <- coef(fit)[["lx"]]
    if (!is.finite(b)) return(NA_real_)
    unname(b)
  }

  # ---------------------------
  # Zone labels (raw + display)
  # ---------------------------
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
  # Category key (VECTOR-SAFE)
  # ---------------------------
  get_category_key <- function(category_base, sex) {
    sx <- canon_sex(sex)
    ifelse(sx %in% c("M","F"),
           paste0(as.character(category_base), "_Sex-", sx),
           as.character(category_base))
  }

  # ---------------------------
  # Crosswalk normalization
  # ---------------------------
  normalize_crosswalk <- function(crosswalk_dt, epoch_keep) {
    CW0 <- as.data.table(crosswalk_dt)
    setnames(CW0, tolower(names(CW0)))

    req <- c("anchor","epoch_sec","category","metric","zone",
             "zone_lower","zone_upper","mean_nhanesw","se_nhanesw","total_min")
    miss <- setdiff(req, names(CW0))
    if (length(miss)) stop("ref_metrics_crosswalks missing columns: ", paste(miss, collapse = ", "))

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

    CW <- CW[epoch_sec == as.integer(epoch_keep)]
    CW
  }

  # For assigning zones to OBS bins, we only need default metric rows (metric==anchor)
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

    list(LUT = LUT)
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

    # Minimal = exact equality at its single-point boundary
    zmin <- zones_dt[zones_dt$band == "Minimal intensity zone", ]
    if (nrow(zmin)) {
      lo <- zmin$intensity_lower[1]
      if (is.finite(lo)) out[is.finite(x) & abs(x - lo) <= tol] <- "Minimal intensity zone"
    }

    # Remaining: (lo, hi]
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

  # ---------------------------
  # Output2 reference joiners:
  #   - default metric rows: metric==anchor  -> nhanes_anchor_*
  #   - peers: metric!=anchor -> equated_<metric>_mean_nhanesw / equated_<metric>_se_nhanesw
  # ---------------------------
  attach_output2_refs <- function(zone_daily, CW) {
    ZD <- copy(zone_daily)

    ref_default <- CW[metric == anchor & (zone %in% c(band6_levels_raw, "All"))]
    setkey(ref_default, anchor, epoch_sec, category_key, zone)

    ZD[, zone_join := fcase(intensity_zone_raw == "Volume", "All", default = intensity_zone_raw)]
    setkey(ZD, anchor, epoch_sec, category_key, zone_join)

    ZD <- ref_default[ZD,
                      on = .(anchor, epoch_sec, category_key, zone = zone_join),
                      nomatch = 0L
    ][, .(
      id, day, metric, epoch_sec, location, sex, age, category, category_key, anchor,
      intensity_zone_raw,
      minutes_in_zone, observed_intensity,
      nhanes_anchor_zone_lower    = zone_lower,
      nhanes_anchor_zone_upper    = zone_upper,
      nhanes_anchor_mean_nhanesw  = mean_nhanesw,
      nhanes_anchor_se_nhanesw    = se_nhanesw,
      nhanes_anchor_total_min_ref = total_min_ref
    )]

    peers <- CW[metric != anchor & (zone %in% c(band6_levels_raw, "All"))]
    peers[, zone_join := zone]
    peers[, peer_metric := metric]

    peers_w <- dcast(
      peers,
      anchor + epoch_sec + category_key + zone_join ~ peer_metric,
      value.var = c("mean_nhanesw","se_nhanesw")
    )

    for (m in setdiff(unique(peers$peer_metric), character(0))) {
      if (paste0("mean_nhanesw_", m) %in% names(peers_w))
        setnames(peers_w, paste0("mean_nhanesw_", m), paste0("equated_", m, "_mean_nhanesw"))
      if (paste0("se_nhanesw_", m) %in% names(peers_w))
        setnames(peers_w, paste0("se_nhanesw_", m), paste0("equated_", m, "_se_nhanesw"))
    }

    setkey(peers_w, anchor, epoch_sec, category_key, zone_join)
    ZD[, zone_join := fcase(intensity_zone_raw == "Volume", "All", default = intensity_zone_raw)]
    ZD <- merge(ZD, peers_w, by = c("anchor","epoch_sec","category_key","zone_join"), all.x = TRUE, sort = FALSE)
    ZD[, zone_join := NULL]

    ZD[]
  }

  # ---------------------------
  # Output3 (Percentiles): anchor percentile + NHANES values at that anchor percentile
  #   Columns produced:
  #     anchor_value_obs
  #     anchor_nearest_centile
  #     nhanes_<m>_value_at_anchor_centile
  #     nhanes_<m>_centile_used   (debug / transparency)
  # ---------------------------
  # ---------------------------
  # Output3 (Percentiles): anchor-centric NHANES lookups
  #
  # For each (id,day,anchor,stat):
  #   1) Find where the observed anchor value ranks in NHANES (nearest centile).
  #   2) Pull NHANES values for ALL metrics at that same centile, under the SAME anchor.
  #
  # Output columns:
  #   anchor_value_obs
  #   anchor_nearest_centile
  #   nhanes_<metric>_value_at_anchor_centile
  #   nhanes_<metric>_centile_used
  # ---------------------------
  build_output3_nhanes_anchor <- function(all_daily, perc_dt) {
    stopifnot(requireNamespace("data.table", quietly = TRUE))
    library(data.table)

    # ---- canonical metric/anchor mapping (must match your package conventions)
    canon_metric <- function(x) {
      x <- tolower(trimws(as.character(x)))
      x[x %in% c("mims", "mims_unit")] <- "mims_unit"
      x[x %in% c("ac", "counts", "vm", "vector.magnitude", "vector_magnitude",
                 "vector_magnitude_counts", "actigraph counts")] <- "ac"
      x
    }
    canon_anchor <- function(x) canon_metric(x)

    canon_sex <- function(x) {
      sx <- toupper(trimws(as.character(x)))
      sx[sx %in% c("MALE","MAN")] <- "M"
      sx[sx %in% c("FEMALE","WOMAN")] <- "F"
      sx[is.na(sx) | !nzchar(sx)] <- "All"
      sx
    }

    # ---- core set for percentiles file (matches your reference tables)
    core <- c("ac", "enmo", "mad", "mims_unit")

    # ---- observed daily stats (from bins aggregation)
    A <- as.data.table(all_daily)
    A[, metric := canon_metric(metric)]
    A <- A[metric %in% core]

    # demographics for lookup (Output3 requires age)
    A[, sex_name := canon_sex(sex)]
    A[, age_i := suppressWarnings(as.integer(round(as.numeric(age))))]
    A <- A[is.finite(age_i)]

    A[, age_range := fifelse(age_i >= 3 & age_i <= 19, "Age 3-19", "Age 20-80")]

    # anchor = observed metric (each metric present becomes an anchor row)
    A[, anchor := canon_anchor(metric)]

    # long observed values for AA + IG
    obs_long <- rbindlist(list(
      A[, .(id, day, epoch_sec, location, sex_name, age_i, age_range,
            anchor, stat = "Average acceleration", anchor_value_obs = as.numeric(overall_intensity))],
      A[, .(id, day, epoch_sec, location, sex_name, age_i, age_range,
            anchor, stat = "Intensity gradient", anchor_value_obs = as.numeric(ig_slope))]
    ), use.names = TRUE, fill = TRUE)

    obs_long <- obs_long[is.finite(anchor_value_obs)]

    # ---- reference table prep
    P0 <- as.data.table(perc_dt)
    setnames(P0, tolower(names(P0)))

    req <- c("epoch_sec","anchor","metric","sex_name","age_range","stat","perc","age","value")
    miss <- setdiff(req, names(P0))
    if (length(miss)) stop("ref_metrics_volume_ig_percentiles missing columns: ", paste(miss, collapse = ", "))

    P <- P0[, .(
      epoch_sec  = as.integer(epoch_sec),
      anchor     = canon_anchor(anchor),
      metric     = canon_metric(metric),
      sex_name   = canon_sex(sex_name),
      age_range  = as.character(age_range),
      stat       = as.character(stat),
      centile    = suppressWarnings(as.numeric(gsub("[^0-9.]+", "", as.character(perc)))),
      age_i      = suppressWarnings(as.integer(age)),
      value_ref  = suppressWarnings(as.numeric(value))
    )]

    P <- P[is.finite(epoch_sec) & is.finite(centile) & is.finite(age_i) & is.finite(value_ref)]
    P <- P[anchor %in% core & metric %in% core]
    setkey(P, anchor, metric, epoch_sec, sex_name, age_range, stat, age_i)

    # ---- helpers (NO .. scoping, uses keyed joins)

    # nearest centile for anchor observed value under anchor->anchor curve
    nearest_centile_anchor <- function(anc, ep, sx, ar, st, ag, vobs) {
      if (!is.finite(vobs)) return(NA_real_)
      S <- P[.(anc, anc, ep, sx, ar, st, as.integer(ag))]
      if (!nrow(S)) return(NA_real_)
      S$centile[which.min(abs(S$value_ref - vobs))]
    }

    # value at a target centile for any metric under the anchor-conditioned curve
    value_at_centile <- function(anc, met, ep, sx, ar, st, ag, cent_target) {
      if (!is.finite(cent_target)) return(list(value_ref=NA_real_, centile_used=NA_real_))
      S <- P[.(anc, met, ep, sx, ar, st, as.integer(ag))]
      if (!nrow(S)) return(list(value_ref=NA_real_, centile_used=NA_real_))

      # exact if possible
      ex <- S[centile == cent_target]
      if (nrow(ex)) return(list(value_ref = ex$value_ref[1], centile_used = cent_target))

      # else snap to nearest available centile
      j <- which.min(abs(S$centile - cent_target))
      list(value_ref = S$value_ref[j], centile_used = S$centile[j])
    }

    # ---- main loop: per observed anchor row, compute anchor centile, then pull NHANES values for all metrics
    out_list <- vector("list", nrow(obs_long))

    for (i in seq_len(nrow(obs_long))) {
      r <- obs_long[i]

      anc <- r$anchor
      ep  <- r$epoch_sec
      sx  <- r$sex_name
      ar  <- r$age_range
      st  <- r$stat
      ag  <- r$age_i
      vobs <- r$anchor_value_obs

      c_anc <- nearest_centile_anchor(anc, ep, sx, ar, st, ag, vobs)

      row_out <- as.list(r[, .(id, day, epoch_sec, location, sex_name, age = age_i, age_range, anchor, stat)])
      row_out$anchor_value_obs <- as.numeric(vobs)
      row_out$anchor_nearest_centile <- as.numeric(c_anc)

      # For each metric, pull the NHANES value at the anchor centile under this anchor
      for (m in core) {
        vm <- value_at_centile(anc, m, ep, sx, ar, st, ag, c_anc)
        row_out[[paste0("nhanes_", m, "_value_at_anchor_centile")]] <- as.numeric(vm$value_ref)
        row_out[[paste0("nhanes_", m, "_centile_used")]] <- as.numeric(vm$centile_used)
      }

      out_list[[i]] <- as.data.table(row_out)
    }

    out <- rbindlist(out_list, use.names = TRUE, fill = TRUE)
    setorder(out, id, day, anchor, stat)
    out[]
  }




  # ---------------------------
  # Weekly (unchanged, simple)
  # ---------------------------
  ua_make_weekly <- function(all_daily) {
    D <- as.data.table(all_daily)
    D[, week := as.integer((day - 1L) %/% 7L) + 1L]
    D[, .(
      n_days = .N,
      total_min_day_mean = mean(total_min_day, na.rm=TRUE),
      overall_intensity_mean = mean(overall_intensity, na.rm=TRUE),
      ig_slope_mean = mean(ig_slope, na.rm=TRUE)
    ), by=.(id, metric, epoch_sec, location, sex, age, category, week)]
  }

  # ============================================================
  # RUN
  # ============================================================
  DT <- as.data.table(load_precomputed_metrics(in_path,
                                               apply_valid_filters = TRUE,
                                               valid_day_hours = 16,
                                               valid_week_days = 0))


  loader_meta <- attr(DT, "ua_loader_meta")
  if (!is.list(loader_meta)) loader_meta <- list()

  if (!("id" %in% names(DT))) stop("No 'id'.")
  if (!("time" %in% names(DT))) stop("No 'time'.")

  epoch_sec <- ua_stop_epoch(ua_pick_epoch(DT))

  if (!("sex" %in% names(DT))) DT[, sex := "All"]
  if (!("age" %in% names(DT))) DT[, age := NA_integer_]

  DT[, sex := canon_sex(sex)]
  DT[, age := suppressWarnings(as.integer(round(as.numeric(age))))]
  DT[, category := vapply(age, ua_infer_category, character(1))]

  demo_id <- DT[, .(
    sex      = sex[!is.na(sex) & nzchar(sex)][1],
    age      = age[is.finite(age)][1],
    category = category[!is.na(category) & nzchar(category)][1]
  ), by=.(id)]
  demo_id[is.na(sex) | !nzchar(sex), sex := "All"]
  demo_id[, category_key := get_category_key(category, sex)]

  CW <- normalize_crosswalk(ref_metrics_crosswalks, epoch_keep = epoch_sec)

  L <- build_zone_LUTs_for_assignment(CW)
  LUT <- L$LUT

  # -----------------------------
  # Output1: bins + assigned intensity zones
  # -----------------------------
  bins <- as.data.table(ua_assign_bins(
    DT        = DT,
    epoch_sec = epoch_sec,
    location  = location,
    per_day   = TRUE,
    id_col    = "id",
    time_col  = "time"
  ))
  if (!nrow(bins)) stop("ua_assign_bins() returned 0 rows.")

  bins <- merge(bins, demo_id[, .(id, sex, age, category, category_key)], by="id", all.x=TRUE, sort=FALSE)
  bins[, metric := canon_metric(metric)]
  bins[, anchor := canon_anchor(metric)]

  bins[, intensity_zone_raw := {
    zdt <- get_zones_for_group(LUT, anchor[1], epoch_sec[1], category_key[1])
    if (is.null(zdt) || !nrow(zdt)) {
      rep(NA_character_, .N)
    } else {
      assign_bands_from_zones(midpoint, zdt)
    }
  }, by=.(id, day, metric, epoch_sec, anchor, category_key)]

  out1_path <- ua_write_csv(bins, file.path(run_folder, sprintf("UA_Output1_BINS_epoch%ss_%s.csv", epoch_sec, run_date)))

  # -----------------------------
  # Daily stats (needed for Output2 Volume + Output3)
  # -----------------------------
  all_daily <- bins[, .(
    total_min_day     = sum(time_bin_min, na.rm=TRUE),
    overall_intensity = ua_overall_intensity(midpoint, time_bin_min),
    ig_slope          = ua_compute_ig_slope(midpoint, time_bin_min)
  ), by=.(id, day, metric, epoch_sec, location)]
  all_daily <- merge(all_daily, demo_id[, .(id, sex, age, category, category_key)], by="id", all.x=TRUE, sort=FALSE)
  all_daily[, metric := canon_metric(metric)]
  all_daily[, anchor := canon_anchor(metric)]

  # -----------------------------
  # Output2: per-day intensity-zone summary
  # -----------------------------
  zone_obs <- bins[!is.na(intensity_zone_raw),
                   .(minutes_in_zone = sum(time_bin_min, na.rm=TRUE),
                     observed_intensity = ua_overall_intensity(midpoint, time_bin_min)),
                   by=.(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw)
  ]
  zone_obs <- merge(zone_obs, demo_id[, .(id, sex, age, category)], by="id", all.x=TRUE, sort=FALSE)

  keys2 <- unique(all_daily[, .(id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category)])
  shell_list <- vector("list", nrow(keys2))
  for (i in seq_len(nrow(keys2))) {
    r <- keys2[i]
    shell_list[[i]] <- data.table(
      id=r$id, day=r$day, metric=r$metric, epoch_sec=r$epoch_sec, location=r$location,
      anchor=r$anchor, category_key=r$category_key, sex=r$sex, age=r$age, category=r$category,
      intensity_zone_raw = band6_levels_raw
    )
  }
  zone_shell <- rbindlist(shell_list, use.names=TRUE, fill=TRUE)

  zone_daily <- merge(
    zone_shell,
    zone_obs[, .(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw,
                 minutes_in_zone, observed_intensity)],
    by=c("id","day","metric","epoch_sec","location","anchor","category_key","intensity_zone_raw"),
    all.x=TRUE, sort=FALSE
  )
  zone_daily[is.na(minutes_in_zone), minutes_in_zone := 0]

  vol <- all_daily[, .(
    id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category,
    intensity_zone_raw = "Volume",
    minutes_in_zone = total_min_day,
    observed_intensity = overall_intensity
  )]
  zone_daily2 <- rbindlist(list(zone_daily, vol), use.names=TRUE, fill=TRUE)

  zone_daily2 <- attach_output2_refs(zone_daily2, CW)

  zone_daily2[, intensity_zone := strip_intensity_zone_phrase(intensity_zone_raw)]
  zone_daily2[, intensity_zone_raw := NULL]

  zone_order_levels <- c("Volume", strip_intensity_zone_phrase(band6_levels_raw))
  zone_daily2[, zone_order := match(intensity_zone, zone_order_levels)]
  setorder(zone_daily2, id, day, metric, zone_order)
  zone_daily2[, zone_order := NULL]

  keep_front <- c(
    "id","day","epoch_sec","location",
    "metric","anchor",
    "sex","age","category","category_key",
    "intensity_zone",
    "minutes_in_zone","observed_intensity",
    "nhanes_anchor_zone_lower","nhanes_anchor_zone_upper",
    "nhanes_anchor_mean_nhanesw","nhanes_anchor_se_nhanesw","nhanes_anchor_total_min_ref"
  )
  equated_cols <- grep("^equated_", names(zone_daily2), value=TRUE)
  out2 <- zone_daily2[, c(keep_front, equated_cols), with=FALSE]

  out2_path <- ua_write_csv(out2, file.path(run_folder, sprintf("UA_Output2_SUMMARY_epoch%ss_%s.csv", epoch_sec, run_date)))

  # -----------------------------
  # Output3: percentiles (AA + IG) with EQUATED values across metrics on each anchor
  # -----------------------------
  out3 <- build_output3_nhanes_anchor(all_daily, ref_metrics_volume_ig_percentiles)

  out3_path <- ua_write_csv(out3, file.path(run_folder, sprintf("UA_Output3_PERCENTILES_epoch%ss_%s.csv", epoch_sec, run_date)))

  # -----------------------------
  # Output4: weekly
  # -----------------------------
  out4_path <- NA_character_
  if (isTRUE(make_weekly)) {
    weekly <- ua_make_weekly(all_daily)
    out4_path <- ua_write_csv(weekly, file.path(run_folder, sprintf("UA_Output4_WEEKLY_epoch%ss_%s.csv", epoch_sec, run_date)))
  }

  # -----------------------------
  # Output5: plain-language run report + glossary
  #   (UPDATED for new Output3 semantics: NHANES values at anchor percentile)
  # -----------------------------
  `%||%` <- function(x, y) if (is.null(x)) y else x

  out5_path <- file.path(run_folder, sprintf("UA_Output5_LOGISTICS_epoch%ss_%s.txt", epoch_sec, run_date))

  metrics_line <- paste(loader_meta$detected_metrics_num %||% character(0), collapse = ", ")
  if (!nzchar(metrics_line)) metrics_line <- "(none detected)"

  out5_lines <- c(
    "universalaccel — end-to-end run report",
    paste0("Run date: ", run_date),
    paste0("Input file: ", in_path),
    paste0("Output folder (this run): ", run_folder),
    paste0("Epoch length: ", epoch_sec, " seconds"),
    paste0("Location: ", location),
    "",
    "============================================================",
    "RUN SUMMARY (EASY VERSION)",
    "============================================================",
    "",
    "What you provided:",
    paste0("  • File name: ", basename(loader_meta$source_path %||% in_path)),
    paste0("  • File type: ", loader_meta$source_ext %||% NA_character_),
    paste0("  • Rows / Columns: ", (loader_meta$n_rows %||% nrow(DT)), " / ", (loader_meta$n_cols %||% ncol(DT))),
    paste0("  • Participants (unique IDs): ", loader_meta$n_unique_id %||% data.table::uniqueN(DT$id)),
    paste0("  • Time span (UTC): ", (loader_meta$time_min_utc %||% NA_character_), "  to  ", (loader_meta$time_max_utc %||% NA_character_)),
    "",
    "Metrics detected in your file:",
    paste0("  • ", metrics_line),
    "",
    "What this tool did (in plain words):",
    "  1) It summarizes each day into intensity bins (how much time you spent at each intensity level).",
    "  2) It groups those bins into 6 movement zones (plus a full-day 'Volume' summary row).",
    "  3) It adds reference values from NHANES so your results have population context.",
    "  4) It creates two NHANES-based comparison views:",
    "       • Zone-based comparisons (Output2)",
    "       • Percentile-based comparisons (Output3)",
    "",
    "Important limitations:",
    "  • The file must have a single epoch length (5s or 60s). Mixed epochs are not supported.",
    "  • If age/sex are missing, the tool uses pooled NHANES references (less specific).",
    "",
    "------------------------------------------------------------",
    "How to interpret NHANES columns (very important)",
    "------------------------------------------------------------",
    "",
    "OUTPUT 2 (zones):",
    "  • Each row is one person × day × metric × zone.",
    "  • 'nhanes_anchor_*' columns are NHANES reference values for the SAME metric as the row’s anchor (same units).",
    "  • 'equated_*' columns are NHANES reference MEAN/SE for OTHER metrics, but mapped to the SAME anchor-defined zone.",
    "    This is the 'equating' idea in Output2: different metrics, comparable zones (same anchor + same zone definition).",
    "",
    "OUTPUT 3 (percentiles):",
    "  • Output3 is anchor-conditioned and percentile-based (not zone-based).",
    "  • Step A: For each person-day and each anchor, it finds where YOUR anchor value ranks in NHANES (nearest percentile).",
    "  • Step B: It then looks up the NHANES values for the OTHER metrics at that SAME percentile,",
    "            using the SAME anchor panel (same anchor, same age, same sex, same epoch, same stat).",
    "  • These Output3 nhanes_* values are population reference values (NHANES), not your observed values for those other metrics.",
    "",
    "If you only provided one metric:",
    "  • Output1/Output2 summarize only what you provided (because they are based on your observed bins/zones).",
    "  • Output3 still works for that anchor: it can still compute your anchor percentile and then pull NHANES values",
    "    for the other metrics at that percentile (because that comes from the reference table).",
    "",
    "============================================================",
    "OUTPUT FILES (WHAT EACH FILE IS FOR)",
    "============================================================",
    "",
    "OUTPUT 1 — UA_Output1_BINS_*.csv",
    "  A detailed distribution table for each person and day (minutes across intensity bins).",
    "  Use it when you want to inspect binning and zone assignment at the bin level.",
    "",
    "OUTPUT 2 — UA_Output2_SUMMARY_*.csv",
    "  A daily summary by movement zone (plus 'Volume' for the full day).",
    "  Key columns:",
    "    • minutes_in_zone      (observed)",
    "    • observed_intensity   (observed)",
    "    • nhanes_anchor_*      (NHANES reference for the anchor metric)",
    "    • equated_*            (NHANES reference for other metrics on the same anchor-defined zone)",
    "",
    "OUTPUT 3 — UA_Output3_PERCENTILES_*.csv",
    "  A daily percentile positioning file for two stats:",
    "    • Average acceleration",
    "    • Intensity gradient",
    "  Key columns:",
    "    • anchor_value_obs",
    "    • anchor_nearest_centile",
    "    • nhanes_<metric>_value_at_anchor_centile",
    "    • nhanes_<metric>_centile_used  (the centile actually used; typically equals anchor_nearest_centile)",
    "",
    "OUTPUT 4 — UA_Output4_WEEKLY_*.csv (optional)",
    "  Simple weekly averages across days for each metric (useful for quick plots/models).",
    "",
    "============================================================",
    "COLUMN GLOSSARY (SHORT)",
    "============================================================",
    "id           : participant ID",
    "day          : day number within the file (1,2,3,...)",
    "epoch_sec    : epoch length (5 or 60 seconds)",
    "location     : sensor location label (e.g., 'ndw')",
    "metric       : the metric being summarized (e.g., ac, enmo, mad, mims_unit)",
    "anchor       : the metric that defines the NHANES lookup panel",
    "sex_name     : M / F / All",
    "age          : integer age",
    "age_range    : Age 3-19 or Age 20-80 (for Output3 lookups)",
    "Volume       : full-day summary (all intensities combined)",
    "",
    "============================================================",
    "END",
    "============================================================"
  )

  ua_write_text(out5_lines, out5_path)

}
