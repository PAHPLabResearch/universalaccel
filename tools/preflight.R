# tools/preflight.R
cat("\n========== universalaccel :: preflight ==========\n")

suppressPackageStartupMessages({
  library(fs)
  library(cli)
  library(withr)
  library(glue)
  library(yaml)
  library(dplyr)
  library(readr)
  library(tibble)
})

root <- path_norm(getwd())
pkg  <- basename(root)

say <- function(..., .type = "info") {
  switch(.type,
         ok   = cli::cli_alert_success(...),
         warn = cli::cli_alert_warning(...),
         err  = cli::cli_alert_danger(...),
         info = cli::cli_alert_info(...)
  )
}

must_exist <- function(p, label = p) {
  if (!fs::file_exists(p)) stop(glue("Missing: {label} ({p})"), call. = FALSE)
}

must_dir <- function(p, label = p) {
  if (!fs::dir_exists(p)) stop(glue("Missing folder: {label} ({p})"), call. = FALSE)
}

# --------------------------------------------------
# 0) Basic structure
# --------------------------------------------------
say("Checking package skeleton in {.path {root}}")
must_exist("DESCRIPTION")
must_exist("NAMESPACE")
must_exist("README.md", "README")
must_exist(".Rbuildignore")
if (!file_exists("LICENSE") && !file_exists("LICENSE.md")) {
  say("No LICENSE found (consider adding one).", .type = "warn")
}

# --------------------------------------------------
# 1) Dependency sanity
# --------------------------------------------------
desc <- readLines("DESCRIPTION", warn = FALSE)
if (!any(grepl("^Package:\\s", desc))) stop("DESCRIPTION lacks Package field.", call. = FALSE)
if (!any(grepl("^Version:\\s", desc))) say("No Version in DESCRIPTION.", .type = "warn")

imports <- gsub("^Imports:\\s*", "", grep("^Imports:", desc, value = TRUE))
if (!length(imports)) say("No Imports listed; ensure dependencies are declared.", .type = "warn")

# --------------------------------------------------
# 2) Load code + required functions exist
# --------------------------------------------------
required_fns <- c(
  # loaders
  "read_and_calibrate_actigraph",
  "read_and_calibrate_axivity",
  "read_and_calibrate_geneactiv",
  # metrics
  "calculate_mims", "calculate_ai", "calculate_enmo",
  "calculate_mad", "calculate_rocam",
  # driver (Part I)
  "accel_summaries",
  # Part II driver
  "ua_run",
  # helpers used by counts/nonwear in docs
  "snap_to_epoch", "clean_id"
)

say("Loading package code in a temporary R session …")
withr::with_envvar(c(R_TESTS = ""), {
  tryCatch({
    devtools::load_all(quiet = TRUE)
    say("load_all() OK", .type = "ok")
  }, error = function(e) {
    stop("devtools::load_all failed: ", e$message, call. = FALSE)
  })
})

missing <- required_fns[!vapply(required_fns, exists, logical(1), mode = "function")]
if (length(missing)) stop("Missing exported or internal functions: ", paste(missing, collapse = ", "), call. = FALSE)
say("Core functions are present.", .type = "ok")

# --------------------------------------------------
# 3) Sample data presence & size caps
# --------------------------------------------------
sx <- tibble::tibble(
  label = c("ActiGraph", "GENEActiv", "Axivity"),
  path  = c(
    "inst/extdata/actigraph/example.gt3x",
    "inst/extdata/geneactiv/TESTfile.bin",
    "inst/extdata/axivity/sample.resampled.csv.gz"
  ),
  max_kb = c(6000, 6000, 1500)  # keep samples modest
)

sx$exists <- fs::file_exists(sx$path)
if (!all(sx$exists)) {
  miss <- sx$label[!sx$exists]
  stop("Sample files missing: ", paste(miss, collapse = ", "), call. = FALSE)
}

sx$kb <- round(fs::file_size(sx$path) / 1024)
print(sx, n = Inf)

too_big <- as.numeric(sx$kb) > sx$max_kb
if (any(too_big)) {
  big <- sx$label[too_big]
  say(glue("Some samples exceed size budget: {paste(big, collapse=', ')}"), .type = "warn")
} else {
  say("Sample sizes within budget.", .type = "ok")
}

# Part II precomputed sample (explicit file you named)
pre_dir <- "inst/extdata/Precomputed"
must_dir(pre_dir, "Precomputed (Part II)")

pre_base <- "UA_FLASH_sample"
pre_csv  <- fs::path(pre_dir, paste0(pre_base, ".csv"))
must_exist(pre_csv, "Precomputed Part II CSV")

pre_kb <- as.numeric(round(fs::file_size(pre_csv) / 1024))
PRE_MAX_KB <- 2000
if (pre_kb > PRE_MAX_KB) {
  say(glue("Precomputed Part II sample exceeds size budget: {pre_kb} KB (> {PRE_MAX_KB} KB)"), .type = "warn")
} else {
  say(glue("Precomputed Part II sample within budget: {pre_kb} KB"), .type = "ok")
}

# --------------------------------------------------
# 4) Minimal end-to-end Part I run on raw samples
#    Writes to tempdir and ensures outputs are produced
# --------------------------------------------------
out_dir <- fs::path_temp(glue("{pkg}-preflight-out"))
fs::dir_create(out_dir)

run_one <- function(device, data_dir, pattern, epochs = c(60)) {
  say(glue("Running {device} quick test …"))
  files <- fs::dir_ls(data_dir, glob = pattern, fail = FALSE)
  if (!length(files)) stop(glue("No {device} sample found under {data_dir}"), call. = FALSE)

  universalaccel::accel_summaries(
    device        = device,
    data_folder   = data_dir,
    output_folder = out_dir,
    epochs        = epochs,
    sample_rate   = 100,
    dynamic_range = c(-8, 8),
    apply_nonwear = TRUE
  )
}

# ActiGraph
run_one("actigraph", "inst/extdata/actigraph", "*.gt3x", epochs = c(60))
# GENEActiv
run_one("geneactiv", "inst/extdata/geneactiv", "*.bin", epochs = c(60))
# Axivity
run_one("axivity", "inst/extdata/axivity", "*.csv.gz", epochs = c(60))

outs <- fs::dir_ls(out_dir, glob = "*.csv", fail = FALSE)
if (!length(outs)) stop("Preflight produced no Part I output CSVs.", call. = FALSE)
say(glue("Part I outputs:\n- {paste(basename(outs), collapse = '\n- ')}"), .type = "ok")

# --------------------------------------------------
# 4b) Part II smoke test on Precomputed sample
# --------------------------------------------------
say("Running Part II (precomputed analysis) smoke test …")

pre_dir <- "inst/extdata/Precomputed"
must_exist(pre_dir, "Precomputed sample folder")

pre_csv <- fs::path(pre_dir, "UA_FLASH_sample.csv")
must_exist(pre_csv, "Precomputed sample CSV (Part II)")

safe_stub <- "UA_FLASH_sample"
part2_parent <- out_dir  # <-- parent
analysis_part2 <- universalaccel::ua_analyze_precomputed(
  in_path     = pre_csv,
  out_dir     = part2_parent,   # <-- pass parent
  location    = "ndw",
  make_weekly = TRUE,
  overwrite   = TRUE
)

# now check the folder the function created:
part2_dir <- fs::path(out_dir, paste0(safe_stub, "_UA_outputs"))
part2_files <- fs::dir_ls(part2_dir, glob = "*.csv")
if (!length(part2_files)) stop("Part II produced no CSV outputs.")

say(glue("Part II outputs:\n- {paste(basename(part2_files), collapse = '\n- ')}"), .type="ok")


# Validate artifacts exist (robust: don’t assume return structure)
csvs2 <- fs::dir_ls(part2_dir, glob = "*.csv", fail = FALSE)
txts2 <- fs::dir_ls(part2_dir, glob = "*.txt", fail = FALSE)

if (!length(csvs2)) stop("Part II preflight: no CSV outputs written.", call. = FALSE)
if (!length(txts2)) stop("Part II preflight: no TXT report/log written.", call. = FALSE)

# Optional “contract” checks: expect these stubs to exist (tighten if you want)
must_stubs <- c(
  "UA_Output1_BINS_",
  "UA_Output2_SUMMARY_",
  "UA_Output3_PERCENTILES_",
  "UA_Output5_LOGISTICS_"
)
for (stub in must_stubs) {
  has_stub <- any(grepl(stub, basename(csvs2), fixed = TRUE)) || any(grepl(stub, basename(txts2), fixed = TRUE))
  if (!has_stub) stop("Part II preflight: missing expected output stub: ", stub, call. = FALSE)
}

# Weekly is expected because make_weekly=TRUE, but warn (don’t fail) if absent
has_weekly <- any(grepl("UA_Output4_WEEKLY_", basename(csvs2), fixed = TRUE))
if (has_weekly) {
  say("Part II preflight: weekly output found (Output4).", .type = "ok")
} else {
  say("Part II preflight: weekly output not found (Output4).", .type = "warn")
}

say(glue(
  "Part II outputs ({basename(pre_csv)}):\n- {paste(basename(c(csvs2, txts2)), collapse = '\n- ')}"
), .type = "ok")

# --------------------------------------------------
# 5) Optional deep checks (toggle TRUE to enable locally)
# --------------------------------------------------
RUN_RCMD_CHECK <- FALSE
RUN_LINTR      <- FALSE
RUN_PKGDOWN    <- FALSE

if (RUN_RCMD_CHECK) {
  say("Running rcmdcheck (this can take a while) …")
  r <- rcmdcheck::rcmdcheck(error_on = "warning")
  print(r$warnings); print(r$notes)
}

if (RUN_LINTR) {
  say("Running lintr …")
  lintr::lint_package()
}

if (RUN_PKGDOWN) {
  say("Building pkgdown site …")
  pkgdown::build_site(preview = FALSE)
}

say("Preflight finished successfully.", .type = "ok")
cat(glue("\nArtifacts written to: {out_dir}\n"))
cat(glue("Part II artifacts written to: {part2_dir}\n"))
