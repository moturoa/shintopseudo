#' Pseudo-database class
#' @export
#' @importFrom safer encrypt_string
#' @importFrom safer decrypt_string
pseudoDB <- R6::R6Class(
  public = list(
    
    con = NULL,
    project = NULL,
    config = NULL,
    files = NULL,
    dbtable = NULL,
    
    initialize = function(config_file, secret){
      
      cfg <- self$read_config(config_file)
      self$config <- cfg$config
      self$project <- cfg$project
      
      self$files <- names(self$config)
      
      self$check_files_exist()
      
      self$con <- self$open_sqlite()
      self$dbtable <- "datadienst"
      
      private$secret <- secret
      
    },
    
    read_config = function(fn){
      
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
        out$project$saverds <- FALSE
      }
      
      if(is.null(out$project$databasename)){
        out$project$databasename <- "shinto_pseudomaker.sqlite"
      }
      
      return(out)  
    },
    
    check_files_exist = function(){
      
      fns <- file.path(self$project$inputdir, self$files)
      
      ex <- file.exists(fns)
      if(!all(ex)){
        nonex <- fns[!ex]
        #pm_log("Files not found: {paste(nonex, collapse = ', ')}", "fatal")
        stop("Some files not found! Check logs.")
      }
    },
    
    open_sqlite = function(){
      
      path <- self$project$database
      dbname <- self$project$databasename
      
      dir.create(path, showWarnings = FALSE)
      fn <- file.path(path, dbname)
      
      db_try <- try(DBI::dbConnect(RSQLite::SQLite(), fn))
      
      if(inherits(db_try, "try-error")){
        #pm_log("Cannot open SQLite database on path {path}", "fatal")
      } else {
        
        # First use: table does not exist.
        if(!self$dbtable %in% DBI::dbListTables(db)){
          
          db_tab <- tibble::tibble(
            key = character(0),  # varchar
            value = character(0),
            hash = character(0)
          )  
          dbWriteTable(con, self$dbtable, db_tab)
        }
        
        #pm_log("Opened SQLite database ({dbname}) on path {path}")
        return(db_try)
      }
      
    },
    
    vacuum_sqlite = function(){
        
      out <- try(DBI::dbExecute(self$con, "vacuum;"))
      if(inherits(out, "try-error") | out[1] != 0){
        #pm_log("VACUUM command not successful!", "fatal")
      } else {
        #pm_log("Database vacuum complete")
      }
      
    },
    
    close_sqlite = function(){
      DBI::dbDisconnect(self$con)
    },
    
    log = function(what){
      message("no logging yet")
    },
    
    #' @param fn Bare filename to read (full path is read from config)
    read_data = function(fn){
      
      cfg <- self$config[fn]
      
      fn_path <- file.path(self$project$inputdir, fn)
      
      self$read_data_fread(fn_path, 
                           sep = cfg$`csv-separator`, 
                           encoding = cfg$encoding,
                           scrub_quotes = cfg$scrub_quotes)
      
      
    },
    
    read_data_fread = function(fn,
                               quote,
                               sep,
                               fill, 
                               skip,
                               encoding){
      
      tm <- system.time({
        
        out <- try(data.table::fread(fn, 
                                     quote = quote,
                                     sep = sep, 
                                     fill = fill,
                                     skip = skip,
                                     encoding = encoding,
                                     showProgress = FALSE,
                                     colClasses = "character"),
                   silent = TRUE)
      })
      
    return(out)
      
    },
    
    # 
    #   scramble_columns(columns = names(fconf$config$encrypt),
    #                    db = db, 
    #                    db_keys = unlist(fconf$config$encrypt),
    #                    store_key_columns = fconf$store_key_columns
    #   ) %>%
    #   delete_columns(fconf$config$remove) %>%
    #   keep_columns(fconf$config$keep) %>%
    #   date_to_year(fconf$config$date_to_year) %>%
    #   to_age_bracket(fconf$config$to_age_bracket)
    # 
    # # write data
    # out_file <- file.path(.cc$project$outputdir, basename(fn))
    # pm_csv_write(data,  out_file)
    # 
    # if(.cc$project$saverds){
    #   pm_rds_write(data,  out_file)  
    # }
    # 
    # #!! naar functie
    # metadat[[i]] <- tibble(
    #   filename = out_file,
    #   md5 = tools::md5sum(out_file),
    #   timestamp = Sys.time()
    # )
    # 
    # # clean up
    # if(.cc$project$clean){
    #   unlink(fn)
    # }
    # 
    
    
    
    
    #' @description Symmetrically encrypt a vector using the secret
    #' @param x A character vector
    encrypt = function(x){
      
      vapply(x, safer::encrypt_string, key = private$secret,
             USE.NAMES = FALSE, FUN.VALUE = character(1)
      )
    },
    
    #' @description Symmetrically decrypt an encrypted vector using the secret
    #' @param x A character vector
    decrypt = function(x){
      
      vapply(x, safer::decrypt_string, key = private$secret,
             USE.NAMES = FALSE, FUN.VALUE = character(1)
      )
    }
  ),
  
  private = list(
    
    secret = NULL,
    
    make_hash = function(n = 1, n_phrase = 9){
      txt <- c(letters,LETTERS,0:n_phrase)
      replicate(n,paste(sample(txt, n_phrase), collapse=""))
    }
    
  )
)
