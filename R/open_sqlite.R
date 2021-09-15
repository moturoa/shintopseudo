#' Open the SQLite
#' @export
pm_open_sqlite <- function(.cc){
  
  path <- .cc$project$database
  dbname <- .cc$project$databasename
  dir.create(path, showWarnings = FALSE)
  fn <- file.path(path, dbname)
  db_try <- try(DBI::dbConnect(RSQLite::SQLite(), fn))
  
  if(inherits(db_try, "try-error")){
    pm_log("Cannot open SQLite database on path {path}", "fatal")
  } else {
    pm_log("Opened SQLite database ({dbname}) on path {path}")
    return(db_try)
  }
  
}
