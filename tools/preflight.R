# tools/preflight.R
cat("\n========== universalaccel :: preflight ==========\n")

suppressPackageStartupMessages({
  library(fs); library(cli); library(withr); library(glue)
  library(yaml); library(dplyr); library(readr)
})

root <- path_norm(getwd())
pkg  <- basename(root)

say <- function(..., .type = "info") {
  switch(.type,
         ok    = cli::cli_alert_success(...),
         warn  = cli::cli_alert_warning(...),
         err   = cli::cli_alert_danger(...),
         info  = cli::cli_alert_info(...)
  )
}

must_exist <- function(p, label = p) {
  if (!fs::file_exists(p)) stop(glue("Missing: {label} ({p})"), call. = FALSE)
}

# --------------------------------------------------
# 0) Basic structure
# --------------------------------------------------
say("Checking package skeleton in {.path {root}}")
must_exist("DESCRIPTION")
must_exist("NAMESPACE")
must_exist("README.md", "README")
must_exist(".Rbuildignore")
if (!file_exists("LICENSE") && !file_exists("LICENSE.md")) say("No LICENSE found (consider adding one).", .type="warn")

# --------------------------------------------------
# 1) Dependency sanity + build ignore
# --------------------------------------------------
desc <- readLines("DESCRIPTION", warn = FALSE)
if (!any(grepl("^Package:\\s", desc))) stop("DESCRIPTION lacks Package field.")
if (!any(grepl("^Version:\\s", desc))) say("No Version in DESCRIPTION.", .type = "warn")

# Light parse of Imports:
imports <- gsub("^Imports:\\s*", "", grep("^Imports:", desc, value = TRUE))
if (!length(imports)) say("No Imports listed; ensure dependencies are declared.", .type="warn")

# --------------------------------------------------
# 2) Required functions exist
# --------------------------------------------------
required_fns <- c(
  # loaders
  "read_and_calibrate_actigraph",
  "read_and_calibrate_axivity",
  "read_and_calibrate_geneactiv",
  # metrics
  "calculate_mims", "calculate_ai", "calculate_enmo",
  "calculate_mad", "calculate_rocam",
  # driver
  "accel_summaries",
  # helpers used by counts/nonwear in docs
  "snap_to_epoch", "clean_id"
)

say("Loading package code in a temporary R session …")
withr::with_envvar(c(R_TESTS = ""), {
  tryCatch({
    devtools::load_all(quiet = TRUE)
    say("load_all() OK", .type="ok")
  }, error = function(e) {
    stop("devtools::load_all failed: ", e$message)
  })
})

missing <- required_fns[!vapply(required_fns, exists, logical(1), mode="function")]
if (length(missing)) stop("Missing exported or internal functions: ", paste(missing, collapse=", "))

say("Core functions are present.", .type="ok")

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
  stop("Sample files missing: ", paste(miss, collapse=", "))
}

sx$kb <- round(fs::file_size(sx$path) / 1024)
print(sx, n=Inf)
too_big <- sx$kb > sx$max_kb
if (any(too_big)) {
  big <- sx$label[too_big]
  say(glue("Some samples exceed size budget: {paste(big, collapse=', ')}"), .type="warn")
} else {
  say("Sample sizes within budget.", .type="ok")
}

# --------------------------------------------------
# 4) Minimal end-to-end run on samples
#    Writes to tempdir and ensures outputs are produced
# --------------------------------------------------
out_dir <- fs::path_temp(glue("{pkg}-preflight-out"))
fs::dir_create(out_dir)

run_one <- function(device, data_dir, pattern, epochs = c(1, 60)) {
  say(glue("Running {device} quick test …"))
  files <- fs::dir_ls(data_dir, glob = pattern)
  if (!length(files)) stop(glue("No {device} sample found under {data_dir}"))
  # run accel_summaries expects a folder
  universalaccel::accel_summaries(
    device = device,
    data_folder   = data_dir,
    output_folder = out_dir,
    epochs        = c(60),     # or c(1,5,60)
    sample_rate   = 100,       # default; change if you want
    dynamic_range = c(-8, 8),  # default; change if you want
    apply_nonwear = TRUE
  )
}

# ActiGraph
run_one("actigraph", "inst/extdata/actigraph", "*.gt3x")
# GENEActiv
run_one("geneactiv", "inst/extdata/geneactiv", "*.bin")
# Axivity
run_one("axivity", "inst/extdata/axivity", "*.csv.gz")

outs <- fs::dir_ls(out_dir, glob = "*.csv")
if (!length(outs)) stop("Preflight produced no output CSVs.")
say(glue("Outputs:\n- {paste(basename(outs), collapse = '\n- ')}"), .type="ok")

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

say("Preflight finished successfully.", .type="ok")
cat(glue("\nArtifacts written to: {out_dir}\n"))
