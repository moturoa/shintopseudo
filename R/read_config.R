# Read config
#' @export
pm_read_config <- function(fn){
  
  out <- try(yaml::read_yaml(fn), silent = TRUE)
  
  if(inherits(out, "try-error")){
    #pm_log("Error reading config ({fn})", "fatal")
    return(FALSE)
  }
  
  for(i in seq_along(out$config)){
    out$config[[i]]$name <- names(out$config)[i]
  }
  
  if(is.null(out$project$clean)){
    out$project$clean <- FALSE
  }
  
  if(is.null(out$project$saverds)){
    out$project$saverds <- TRUE
  }
  
  if(is.null(out$project$databasename)){
    out$project$databasename <- "shinto_pseudomaker.sqlite"
  }
  
  return(out)  
}
