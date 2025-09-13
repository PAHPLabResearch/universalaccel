#' @export
run_calibrated_data_app <- function() {
  app_pkgs <- c(
    "shiny", "bslib", "DT", "dplyr", "tidyr", "purrr", "readr",
    "ggplot2", "scales", "gridExtra", "equivalence",
    "officer", "flextable", "base64enc"
  )
  
  # install any missing app packages
  missing <- app_pkgs[!vapply(app_pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(missing)) {
    message("Installing missing app dependencies: ", paste(missing, collapse = ", "))
    install.packages(missing)
  }
  
  app_dir <- system.file("shiny", "calibrateddata", package = "universalaccel")
  if (!nzchar(app_dir) || !dir.exists(app_dir)) {
    stop(
      "Shiny app directory not found in installed package.\n",
      "Expected at: inst/shiny/calibrateddata\n",
      "Reinstall the package and ensure the 'inst/' folder is included.\n",
      call. = FALSE
    )
  }
  
  shiny::runApp(app_dir, display.mode = "normal")
}
