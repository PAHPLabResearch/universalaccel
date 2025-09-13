#' Launch the packaged Shiny app
#' @export
run_calibrated_data_app <- function() {
  # App-only dependencies (kept in Suggests)
  app_pkgs <- c(
    "shiny","bslib","DT","dplyr","tidyr","purrr","readr",
    "ggplot2","scales","gridExtra","equivalence",
    "officer","flextable","base64enc"
  )
  
  # Check that app dir is present in the installed package
  app_dir <- system.file("shiny", "calibrateddata", package = "universalaccel")
  if (!nzchar(app_dir) || !dir.exists(app_dir)) {
    stop("Can't find the installed app. Ensure 'inst/shiny/calibrateddata' existed when you built the package.")
  }
  
  # Sanity check that the demo data is there
  demo_csv <- file.path(app_dir, "devicefinaldataday.csv")
  if (!file.exists(demo_csv)) {
    stop("Missing 'devicefinaldataday.csv' in the installed app dir: ", app_dir)
  }
  
  # Offer to install missing app dependencies (interactive only)
  missing <- app_pkgs[!vapply(app_pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(missing)) {
    if (interactive()) {
      msg <- paste0(
        "The app needs these packages: ", paste(missing, collapse = ", "),
        "\nInstall them now?"
      )
      ans <- utils::menu(c("Yes", "No"), title = msg)
      if (ans == 1) utils::install.packages(missing)
      missing <- app_pkgs[!vapply(app_pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
      if (length(missing)) stop("Still missing: ", paste(missing, collapse = ", "))
    } else {
      stop("Missing app packages: ", paste(missing, collapse = ", "),
           ". Install them, then re-run run_calibrated_data_app().")
    }
  }
  
  shiny::runApp(app_dir, display.mode = "normal")
}




