
if (!requireNamespace("renv", quietly = TRUE)) install.packages("renv")
renv::restore()
message("[bootstrap] Environment restored from renv.lock")

# quick smoke test (optional)
if (!requireNamespace("universalaccel", quietly = TRUE)) {
  message("[bootstrap] Building/loading package …")
  if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
  devtools::document(quiet = TRUE)
  devtools::load_all(quiet = TRUE)
  message("[bootstrap] Package loaded.")
}
