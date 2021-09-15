#' Check if files exist
#' @export
pm_check_files_exist <- function(conf){
  
  files <- names(conf$config)
  fns <- file.path(conf$project$inputdir, files)
  
  ex <- file.exists(fns)
  if(!all(ex)){
    nonex <- fns[!ex]
    #pm_log("Files not found: {paste(nonex, collapse = ', ')}", "fatal")
    stop("Some files not found! Check logs.")
  }
}
