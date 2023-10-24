#' Pseudo-database class
#' @export
#' @importFrom safer encrypt_string
#' @importFrom safer decrypt_string
#' @importFrom R.utils countLines
#' @importFrom tools md5sum
#' @importFrom data.table fread fwrite
#' @importFrom glue glue
#' @importFrom futile.logger flog.info flog.fatal flog.warn
#' @importFrom DBI dbReadTable dbWriteTable dbListTables
#' @importFrom RSQLite SQLite
#' @importFrom futile.logger flog.appender appender.tee
#' @importFrom lubridate year
#' @importFrom uuid UUIDgenerate
#' @importFrom stringi stri_trans_general
#' @importFrom R.utils countLines
#' @importFrom R6 R6Class
#' @importFrom dplyr filter select all_of any_of tbl as_tibble
#' @importFrom tidyr replace_na
#' @importFrom stringr str_trim
pseudoDB <- R6::R6Class(classname = "pseudoDB",
                        lock_objects = FALSE,
  public = list(
    
    #' @field con Database connection
    con = NULL,
    
    #' @field project Project block from config
    project = NULL,
    
    #' @field config Config block from config
    config = NULL,
    
    #' @field files File names in the configuration file
    files = NULL,
    
    #' @field datalog Dataframe with file statistics. Filled before and during processing.
    datalog = NULL,
    
    #' @description Make a new object of class 'pseudoDB'. When initializing, reads the configuration
    #' file, checks and makes the output directories specified in the configuration file, opens a connection
    #' to the sqlite ('shinto_pseudomaker.sqlite'), checks if all files in the config exist (if not, they are skipped later).
    #' @param config_file Path to the YML file with settings
    #' @param secret Secret key used for (extra) symmetric encryption
    #' @param log_to Log to a file or stdout (pertains to old logging in .log files, see shintopseudo.csv in the file output folder(s)).
    #' @param max_n_lines Max number of lines to read from the input files; used for testing only
    initialize = function(config_file, secret, 
                          log_to = c("file","stdout"),
                          max_n_lines = NULL){
      
      log_to <- match.arg(log_to)
      
      # Read configuration file, and take 'project' and 'config' blocks
      cfg <- self$read_config(config_file)
      self$config <- cfg$config
      self$project <- cfg$project
      
      self$max_n_lines <- max_n_lines
      self$files <- names(self$config)
      
      self$check_files_exist()
      
      private$secret <- secret
      
      # create output directories from project config if they don't exist
      self$create_directories()
      
      if(log_to == "file"){
        self$open_logfile()
      }
      
      # Open connection to SQLite
      self$dbtable <- "datadienst"
      self$con <- self$open_sqlite()
      
      # Start the datalog; will be filled further during processing.
      # This gets saved to shintopseudo.csv in the file output directory.
      self$datalog <- tibble::tibble(
        id = uuid::UUIDgenerate(),
        projectname = self$project$projectname,
        timestamp_start = paste(Sys.time()),
        file = self$files,
        path_in = self$project$inputdir,
        path_out = self$project$outputdir,
        status = character(1),
        error_code = "",
        timestamp = character(1),
        md5_in = private$checksum(file.path(self$project$inputdir, self$files)),
        md5_out = character(1),
        n_lines_file = integer(1),
        n_rows_read = integer(1),
        n_cols_read = integer(1),
        encrypted_columns = private$encrypted_columns()
      )
      
    },
    
    #' @description Create output/log/sqlite directories if not exist
    create_directories = function(){
      dir.create(self$project$outputdir, showWarnings = FALSE)
      dir.create(self$project$database, showWarnings = FALSE)
      dir.create(self$project$logdir, showWarnings = FALSE)
    },
    
    #' @description Opens a log file in the log output directory
    open_logfile = function(){
      
      today_ <- format(Sys.Date(), "%Y%m%d")
      fn <- file.path(self$project$logdir, 
                      paste0(today_, "_pseudomaker.log"))
      
      futile.logger::flog.appender(futile.logger::appender.tee(fn), name = "pseudomaker")
      self$log("------------- start pseudoDB v. {packageVersion('shintopseudo')} -------------")

    },
    
    #' @description Writes shintopseudo.csv in the file output directory
    write_datalog = function(){
      
      path <- file.path(self$project$outputdir, 
                        "shintopseudo.csv")
                     
      write.csv2(self$datalog, path, row.names = FALSE)
      
    },
    
    #' @description Logs to the old-style logging file
    #' @param msg Logging message
    #' @param how Either info, fatal or warn
    log = function(msg, how = c("info","fatal","warn")){
      
      how <- match.arg(how)
        
      msg <- glue::glue(msg, .envir = parent.frame(n = 1))
      switch(how, 
             fatal = futile.logger::flog.fatal(msg, name = "pseudomaker"),
             info = futile.logger::flog.info(msg, name = "pseudomaker"),
             warn = futile.logger::flog.warn(msg, name = "pseudomaker"))
      
    },
    
    #' @description Update a field in the datalog during processing
    #' @param file For which file to set the datalog
    #' @param what Set which field (column)
    #' @param value Set the value
    set_data_log = function(file, what, value){

      self$datalog[[what]][self$datalog$file == file] <- value
      
    },
    
    #' @description Set the status in the datalog (for e.g. errors)
    #' @param file Filename to set a status
    #' @param status Status to set
    set_status = function(file, status){
      self$set_data_log(file, "status", status)
    },
    
    #' @description Set an error in the data log for a file (and a timestamp)
    #' @param file Filename to flag an error
    #' @param error Error code
    set_error = function(file, error){
      self$set_status(file, "FAIL")
      self$set_data_log(file, "timestamp", as.character(Sys.time()))
      self$set_data_log(file, "error_code", error)
    },
    
    #' @description Reads the config from a .yml/.yaml file
    #' @param fn Path to yml
    read_config = function(fn){
      
      out <- try(yaml::read_yaml(fn), silent = TRUE)
      
      if(inherits(out, "try-error")){
        self$log("Error reading config ({fn})", "fatal")
        return(FALSE)
      }
      
      for(i in seq_along(out$config)){
        out$config[[i]]$name <- names(out$config)[i]
      }
      
      # optional 'clean' setting in project. Not used (?)
      if(is.null(out$project$clean)){
        out$project$clean <- FALSE
      }
      
      
      if(is.null(out$project$databasename)){
        out$project$databasename <- "shinto_pseudomaker.sqlite"
      }
      
      return(out)  
    },
    
    #' @description Check if all files mentioned in the config exist
    check_files_exist = function(){
      
      fns <- file.path(self$project$inputdir, self$files)
      
      ex <- file.exists(fns)
      if(!all(ex)){
        nonex <- fns[!ex]
        self$log("Some files not found: {paste(nonex, collapse = ', ')}", "warn")
      }
    },
    
    #' @description Opens a connection to the SQLite with `DBI::dbConnect(RSQLite::SQLite()...)`,
    #' prepares an empty 'datadienst' table in the database if it does not exist already.
    open_sqlite = function(){
      
      path <- self$project$database
      dbname <- self$project$databasename
      fn <- file.path(path, dbname)
      
      db_try <- try(DBI::dbConnect(RSQLite::SQLite(), fn))
      
      if(inherits(db_try, "try-error")){
        self$log("Cannot open SQLite database on path {path}", "fatal")
        stop("SQLite connection failed.")
      } else {
        
        # First use: table does not exist.
        if(!self$dbtable %in% DBI::dbListTables(db_try)){
          
          db_tab <- tibble::tibble(
            key = character(0),  # varchar
            value = character(0),
            hash = character(0)
          )  
          DBI::dbWriteTable(db_try, self$dbtable, db_tab)
        }
        
        self$log("Opened SQLite database ({dbname}) on path {path}")
        return(db_try)
      }
      
    },
    
    #' @description Performs a vacuum on the SQLite. Automatically done before closing the connection.
    #' @details from sqlite.org: "The VACUUM command rebuilds the database file, repacking it into 
    #' a minimal amount of disk space [...] Frequent inserts, updates, and deletes can cause the database 
    #' file to become fragmented - where data for a single table or index is scattered around the database file.
    #' Running VACUUM ensures that each table and index is largely stored contiguously within the database file.". 
    vacuum_sqlite = function(){
        
      out <- try(DBI::dbExecute(self$con, "vacuum;"))
      if(inherits(out, "try-error") | out[1] != 0){
        self$log("VACUUM command not successful!", "fatal")
      } else {
        self$log("Database vacuum complete")
      }
      
    },
    
    #' @description Close the DB connection and perform a vacuum
    #' @param vacuum Whether to vacuum the SQLite or not. See $vacuum_sqlite method.
    close_sqlite = function(vacuum = TRUE){
      if(vacuum){
        self$vacuum_sqlite()
      }
      DBI::dbDisconnect(self$con)
      self$log("Database connection terminated.")
    },
    
    #' @description Close everything (also the log file)
    close = function(){
      
      self$write_datalog()
      self$close_sqlite()
      self$log("------------- end pseudoDB -------------")
      
    },
    
    #' @description Reads a file from the config. Includes multiple methods.
    #' @details Normally $read_data_fread is used unless readmethod='json', in which case
    #' the config setting 'post_read_function' is applied to the result of `jsonlite::fromJSON`, 
    #' so that you might attempt to flatten a JSON into a neat CSV. 
    #' @param fn Bare filename to read (full path is read from config). 
    read_data = function(fn){
      
      cfg <- self$config[[fn]]
      
      fn_path <- file.path(self$project$inputdir, fn)
      
      if(!file.exists(fn_path)){
        self$log("File not found: {fn_path}", "warn")
        self$set_error(fn, "File not found")
        return(NULL)
      }
      
      if(is.null(cfg$skip_lines)){
        skip <- "__auto__"
      } else {
        skip <- as.numeric(cfg$skip_lines)
      }
      
      if(is.null(cfg$readfunction) && is.null(cfg$readmethod)){
        tm <- system.time({
          out <- self$read_data_fread(fn_path, 
                                      quote = "\"",
                                      sep = cfg$`csv-separator`, 
                                      fill = TRUE,
                                      skip = skip,
                                      encoding = cfg$encoding)  
        })  
      } else {
        
        if(!is.null(cfg$readfunction)){
          
          flog.info(paste("Starting custom read function:",cfg$readfunction))
          
          if(!exists(cfg$readfunction, mode = "function")){
            self$set_error(fn, paste("readfunction",cfg$readfunction,"not found. Must be present in app/R/ in pseudomaker."))
            return(NULL)
          } else {
            read_fun <- base::get(cfg$readfunction)
            
            tm <- system.time(
              out <- try(read_fun(fn_path))
            )
            
            if(inherits(out, "try-error")){
              self$set_error(fn, "Error in custom read function")
            }
          }  
          
        } else if(!is.null(cfg$readmethod)){
          
          if(cfg$readmethod == "json"){
            
            tm <- system.time(
              out <- jsonlite::fromJSON(fn_path)
            )
            
            fun_code <- cfg[["post_read_function"]]
            
            if(!is.null(fun_code)){
              if(grepl("^function[(]", fun_code)){
                post_read_fun <- eval(parse(text = fun_code))
              } else {
                post_read_fun <- base::get(fun_code)
              }
              
              out <- try(post_read_fun(out))
              if(inherits(out, "try-error")){
                self$set_error(fn, paste("problem executing code: ", fun_code))
              }
            }
            
            
            slot_name <- cfg[["json_features_name"]]
            if(!is.null(slot_name)){
              out <- out[[slot_name]]
            }
            
          } else {
            self$set_error(fn, paste("readmethod",cfg$readmethod,"argument obsolete"))
          }
          
        } 
        
        
      }
      
      if(inherits(out, "try-error")){
        self$set_error(fn, paste0("Error reading raw data: '",as.character(out),"'"))
        return(NULL)
      }
      
      if(nrow(out) == 0){
        self$log("Data read but has no rows - skipped", "warn")
        self$set_error(fn, "Zero rows read from file")
        return(NULL)
      }
      
      out <- private$fix_names(out)
      
      # remove \"
      if(isTRUE(cfg$scrub_quotes)){
        out <- tibble::as_tibble(lapply(out, function(x)gsub("\"", "", x)))  
      }
      
      chk <- private$compare_n_rows(fn_path, out)
      self$set_data_log(fn, "n_lines_file", chk$n_lines)
      self$set_data_log(fn, "n_rows_read", chk$n_rows)
      self$set_data_log(fn, "n_cols_read", ncol(out))
      
      n_lines_data <- chk$n_rows + 1
      if((chk$n_rows + 1) != chk$n_lines){
        self$log(paste0("Not all lines read from data: file has {chk$n_lines} lines,",
                        "data has {chk$n_rows} rows"), "warn")
      }

      mem <- round(object.size(out) * 1E-06, 1)
      self$log("'{fn}' : {chk$n_rows} rows read in {round(tm[3],1)} sec., occupying {mem}MB memory.")
      
      out
    },
    
    
    #' @description Default method to read the CSV using `data.table::fread`.
    #' @param fn Filename WITH full path (unlike `$read_data`)
    #' @param quote Argument 'quote' in fread()
    #' @param sep Argument 'sep' in fread()
    #' @param fill Argument 'fill' in fread()
    #' @param skip Argument 'skip' in fread()
    #' @param encoding Either UTF-8 or Latin-1 (or leave blank for 'unknown', which is not very reliable!)
    read_data_fread = function(fn,
                               quote,
                               sep,
                               fill, 
                               skip = 0,
                               encoding = NULL){
      
      if(is.null(encoding) || encoding == ""){
        encoding <- "unknown"
      }
      
      nrows <- ifelse(is.null(self$max_n_lines), Inf,  self$max_n_lines)
      
      tm <- system.time({
        
        out <- try(data.table::fread(fn, 
                                     quote = quote,
                                     sep = sep, 
                                     fill = fill,
                                     skip = skip,
                                     encoding = encoding,
                                     nrows = nrows,
                                     showProgress = FALSE,
                                     colClasses = "character"),
                   silent = TRUE)
      })
      
      
      
    return(out)
      
    },
    
    #' @description Writes an output CSV with `data.table::fwrite`
    #' @param data Dataframe
    #' @param fn Filename
    write_data = function(data, fn){
      
      path <- file.path(self$project$outputdir, fn)
      
      # avoid list column problem
      # (kan me deze bug niet herinneren maar zal wel nodig zijn) 
      data <- dplyr::as_tibble(apply(data, 2, as.character))
      
      tm <- system.time(
        data.table::fwrite(data, path, sep = ";")  
      )
      
      self$log("'{fn}' : {nrow(data)} lines written in {round(tm[3],1)} sec.")
       
    },
    
    
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
    },
    
    #' @description Symmetric encryption for multiple columns at once
    #' @param data A Dataframe
    #' @param columns Vector of column names
    #' @param new_names Vector of new column names in the output dataframe (to be added 
    #' in addition to the original).
    symmetric_encrypt_columns = function(data, columns, new_names = NULL){
    
      for(i in seq_along(columns)){
        
        out_column <- ifelse(is.null(new_names[i]), columns[i], new_names[i])
        
        data[[out_column]] <- self$encrypt(data[[columns[i]]])  
        
        self$log("{nrow(data)} values symmetric encrypted, column: {columns[i]} to {out_column}") 
        
      }  
      
      
      data  
    },
    
    
    #' @description The most basic function: making a 9-character hash used to make all pseudo-IDs.
    #' @param n Number of hashes to make
    #' @param n_phrase Length of the hash (default = 9 chars)
    make_hash = function(n = 1, n_phrase = 9){
      txt <- c(letters,LETTERS,0:9)
      replicate(n,paste(sample(txt, n_phrase), collapse=""))
    },
    
    
    #' @description Anonymize a column. This is the largest and most crucial method. 
    #' @param data Dataframe
    #' @param column Column name to hash
    #' @param db_key Key name of the column 
    #' @param store_key_columns Special method; do not use.
    #' @param normalise_key_columns Add a normalized ASCII version of the column to the dataframe (special
    #' characters replaced with ASCII 'equivalents')
    #' @param file Unused argument; ignore
    #' @details Replaces every value in the column of the dataframe with 'hashes', so that each same value in 
    #' the data will get the same hash. Values already hashed will be read from the sqlite (so that the same hashes/value)
    #' combinations get made in each file, and each run of the process), values not previously hashed will get a new
    #' value/hash combination which is written to the sqlite.
    anonymize_column = function(data, column, db_key = NULL,
                                store_key_columns = NULL,
                                normalise_key_columns = NULL,
                                file = NULL){
      
      st <- proc.time()[3]
      
      # db_key is for looking up encrypted value/hash pairs from the db.
      # if not provided, use column name itself.
      if(is.null(db_key) || is.na(db_key) || db_key == ""){
        self$log("No key set for {column}, using column name as key - safer to set a key!", "warn")
        db_key <- column
      }
      
      # value/hash pairs
      u <- unique(data[[column]])
      
      u_nospace <- gsub("[[:space:]]", "", u)
      
      # remove values we don't want to hash.
      u <- u[!is.na(u) & u != "" & nchar(u_nospace) > 0]
      
      # key/value/hash table (will refill with already hashed values below)
      key <- tibble(
        key = db_key,
        value = u,
        hash = self$make_hash(length(u))
      )
      
      # encrypt
      key$value <- self$encrypt(key$value)
      
      # Read previous hash/value pairs for this db_key.
      key_db <- dplyr::tbl(self$con, self$dbtable) %>%
        dplyr::filter(key == {{db_key}})
      
      key_db_overlap <- try(
        dplyr::filter(key_db, value %in% !!key$value) %>%
          dplyr::collect(.)
      )
      
      if(inherits(key_db_overlap, "try-error")){
        self$log("Could not read from SQLite.", "fatal")
        stop("Could not read from SQLite.")
      }
      
      if(nrow(key_db_overlap)){
        self$log("{nrow(key_db_overlap)} value/hashes read from SQLite.") 
      }
      
      # New hash/value pairs not previously encrypted.
      key_db_new <- dplyr::filter(key, !value %in% !!key_db_overlap$value)
      
      # Now adjust the key with the db hash/value pairs, if any.
      if(nrow(key_db_overlap) > 0){
        i_m <- match(key$value, key_db_overlap$value)
        if(length(i_m)){
          
          isn <- which(!is.na(i_m))
          
          key$hash[isn] <- key_db_overlap$hash[i_m[isn]]
          
        }  
      }
      
      # 'key_store' wordt alleen voor IZM gebruikt.
      # store_key_columns: alleen aanpassen op basis van 1e entry.
      # (omdat deze stap anders te vaak wordt herhaald)
      if(!is.null(store_key_columns) && column == store_key_columns[1]){
        
        if(!all(store_key_columns %in% names(data))){
          self$log("Not all store_key_columns found in data, skipping saving.", "warn")
        }
        
        self$log("Storing key columns (IZM)")
        
        db_key_col <- names(db_key)
        if(!db_key_col %in% store_key_columns){
          store_key_columns <- c(db_key_col, store_key_columns)
        }

        vals <- dplyr::select(data, all_of(store_key_columns)) %>%
          dplyr::filter(!is.na(!!sym(db_key_col)))
        
        key_store <- key
        key_store$value <- self$decrypt(key_store$value)
        
        key_store <- key_store %>% 
          dplyr::left_join(vals, by = c(value = db_key_col)) %>%
          dplyr::rename(!!db_key_col := value) %>%
          dplyr::select(-key)

        # no duplicate hash allowed; ede-izmrest-api adds an index with unique
        # contraint on the 'hash' column
        # on 2022-6-30 we had 27 duplicates, all with missing 'postcode'
        key_store <- dplyr::distinct(key_store, hash, .keep_all = TRUE)
        
        
        # IZM special
        # Store normalized version of 'name' column for easier searching
        for(keycol in store_key_columns){
          
          these_cols <- names(normalise_key_columns)
          if(!is.null(normalise_key_columns) && keycol %in% these_cols){
            
              # NAme 
              name_output <- normalise_key_columns[[keycol]]
              self$log(glue("Storing normalised form of column: {keycol} to column {name_output}"))
              key_store[[name_output]] <- stringi::stri_trans_general(key_store[[keycol]], id = "Latin-ASCII")
            
          }
          
          # Het voorvoegsel van de achternaam staat apart, die moeten we eerst plakken voordat 
          # we de genormaliseerde versie gaan opslaan.
          if(keycol == "PRSGESLACHTSNAAM"){
            
            vv <- key_store[["PRSVOORVOEGSELGESLACHTSNAAM"]]
            vv[is.na(vv)] <- ""
            
            key_store[["PRSGESLACHTSNAAM"]] <- trimws(paste(vv, key_store[["PRSGESLACHTSNAAM"]]))
            
            # we hebben geen normalised form van het voorvoegsel dus hopen dat er 
            # weinig voorvoegsels zijn zonder speciale tekens ...
            key_store[["PRSGESLACHTSNAAMNORM"]] <- trimws(paste(vv, key_store[["PRSGESLACHTSNAAMNORM"]]))
            

          }
        }
        
        # Write keys
        DBI::dbWriteTable(self$con, "keystore", key_store, overwrite = TRUE)
        
        self$log(glue("Key columns stored ({nrow(key_store)} rows)"))
      }
      
      # Append value/hash to DB (only those rows not previously stored).
      if(nrow(key_db_new) > 0){
        DBI::dbWriteTable(self$con, self$dbtable, key_db_new, append = TRUE)
        #pm_log("{nrow(key_db_new)} value/hashes written to SQLite.")
      }
      
      # Finally assign hashed values.
      key_value <- self$decrypt(key$value)
      
      # should not be necessary, but make sure we have no NA or ""
      key_value <- gsub("[[:space:]]", "", key_value)
      key_value <- key_value[!is.na(key_value) & key_value != ""]
      
      # before looking up, also remove spaces.
      data_column <- gsub("[[:space:]]", "", data[[column]])
      
      hashed <- key$hash[match(data_column, key_value)]
      data[[column]] <- hashed
      
      # Logging
      nd <- proc.time()[3]
      self$log("{nrow(data)} values hashed ({column}) in {round(nd-st,1)} sec.")
      
      return(data)
      
    },
    
    #' @description See $anonymize_column; this is the vectorized version for multiple columns
    #' @param data See $anonymize_column
    #' @param columns See $anonymize_column
    #' @param db_keys See $anonymize_column
    #' @param file See $anonymize_column
    #' @param ... Further passed to $anonymize_column
    anonymize_columns = function(data, columns, db_keys, file, ...){
      
      for(i in seq_along(columns)){
        data <- self$anonymize_column(data, columns[i], db_key = db_keys[i], file=file, ...)
      }
      
      return(data)
    },

    #' @description Only used for a very specific case. Not further encouraged.
    #' @param path Filename
    read_bag_extract = function(path){
      data.table::fread(path, colClasses = "character")
    },
    
    #' @description Only used in a very specific case. Not supported or encouraged.
    #' @param data Dataframe
    #' @param column Column name
    #' @param columns_out Names of output columns
    #' @param bag_path Path to BAG file
    validate_address = function(data, column, columns_out, bag_path){
      
      if(is.null(column))return(data)
      
      bag <- as.data.frame(self$read_bag_extract(bag_path))
      data <- as.data.frame(data)
      
      tm <- system.time({
        out <- validate_address(data = data, 
                                adres_column = column, 
                                bag = bag, 
                                bag_columns = names(columns_out)) %>%
          as.data.frame  
      })
      
      switch_list <- function(x){
        as.list(names(x)) %>% setNames(unlist(x))
      }
      
      nonna <- mean(!is.na(out[[names(columns_out)[1]]]))
      
      self$log("Addresses validated: {round(nonna*100,1)}% success in {round(tm[3],1)} sec.")
      
      dplyr::rename(out, !!!switch_list(columns_out))
      
    },
    
    #' @description Run the entire process. Read files, anonymize, encrypt, write, log.
    #' @param files Optional vector of filenames to process, otherwise processes all in the loaded config.
    process_files = function(files = NULL){

      if(is.null(files))files <- self$files

      for(i in seq_along(files)){

        fn <- files[i]
  
        self$log("Processing {fn} ...")
        
        cfg <- self$config[[fn]]$config
        
        out <- self$read_data(fn) 
        if(is.null(out)){
          self$log("Error reading {fn} - skipping to next file.")
          self$set_error(fn, "File could not be read")
          next
        }
        
        # if(nrow(out) < 3){
        #   self$set_error(fn, "File is (nearly) empty")
        #   next
        # }
        
        # available columns: columns in data + output from validate_address
        data_colnames <- c(names(out), unlist(cfg$validate_address$columns_out))
        
        if(!all(names(cfg$encrypt) %in% data_colnames)){
          
          nm_mis <- setdiff(names(cfg$encrypt), names(out))
          
          self$log("Columns not found: {paste(nm_mis, collapse=',')}.", "fatal")
          self$log("Available columns: {paste(names(out), collapse = ',')}", "fatal")
          self$set_error(fn, "Column(s) not found in data")
          next
        }
        
        # Run all steps one by one
        out <- out %>%
          # validate address before anything else (specific case, rarely used)
          self$validate_address(column = cfg$validate_address$column, 
                                columns_out = cfg$validate_address$columns_out, 
                                bag_path = cfg$validate_address$bag_path) %>%
          # anonymize columns (encrypt config block)
          self$anonymize_columns(columns = names(cfg$encrypt),
                            db_keys = unlist(cfg$encrypt),
                            store_key_columns = self$config[[fn]]$config$store_key_columns,
                            normalise_key_columns = self$config[[fn]]$config$normalise_key_columns,
                            file = fn) %>%
          # symmetric encryption with libsodium (symmetric block in config)
          self$symmetric_encrypt_columns(columns = names(cfg$symmetric),
                                         new_names = unlist(cfg$symmetric)) %>%
          # remove columns
          self$delete_columns(cfg$remove) %>%
          
          # only keep these columns in the output
          self$keep_columns(cfg$keep) %>%
          
          # convert date to year (unused)
          self$date_to_year(cfg$date_to_year) %>%
          
          # age to 10-15, 15-20, etc.
          self$to_age_bracket(cfg$to_age_bracket)

        # Write output file
        self$write_data(out, fn)
        
        # write more datalog outputs
        chk <- private$checksum(file.path(self$project$outputdir, fn))
        self$set_data_log(fn, "md5_out", chk)
        self$set_data_log(fn, "timestamp", as.character(Sys.time()))
        self$set_status(fn, "OK")
        
        
      }

    },
    
    #' @description Specific for dd-mm-yyyy dates in the data; not configurable
    #' (and not used in any application)
    #' @param data Dataframe
    #' @param column Name of column
    date_to_year = function(data, column){
      
      for(i in seq_along(column)){
        dts <- as.Date(data[[column[i]]], format = "%d-%m-%Y")  #????
        if(all(is.na(dts))){
          data[[column[i]]] <- substr(data[[column[i]]], 1, 4)
        } else {
          data[[column[i]]] <- as.character(lubridate::year(dts))  
        }
        
      }
      
      data
    },
    
    #' @description Age in years to bracket (5-10, 10-15 etc.)
    #' @param data Dataframe
    #' @param columns Name of columns
    to_age_bracket = function(data, columns){
      
      if(length(columns) == 0)return(data)
      
      brks <- seq(0,150,by=5)
      n <- length(brks)
      labs <- paste(brks[1:(n-1)], "-", brks[2:n])
      
      for(i in seq_along(columns)){
        
        data[[columns[i]]] <- as.character(cut(as.numeric(data[[columns[i]]]), 
                                               breaks = brks,
                                               labels = labs))
      }
      
      data
    },
    
    #' @description Keep these columns
    #' @param data Dataframe
    #' @param columns Name of columns
    keep_columns = function(data, columns){
      if(length(columns) == 0)return(data)
      columns <- private$existing_columns(columns, data)
      dplyr::select(data, dplyr::any_of(columns))
    },
    
    #' @description Delete these columns
    #' @param data Dataframe
    #' @param columns Name of columns
    delete_columns = function(data, columns){
      if(length(columns) == 0)return(data)
      columns <- private$existing_columns(columns, data)
      dplyr::select(data, -dplyr::any_of(columns))
    }
    

    
  ),
  
  # Private functions not documented. Helpers/utilities.
  private = list(

    encrypted_columns = function(){
      unname(sapply(lapply(lapply(self$config, "[[", "config"), "[[", "encrypt"), jsonlite::toJSON))  
    },
    
    fix_names = function(x){
      
      new_nms <- gsub("\"","", names(x))
      new_nms[new_nms == ''] <- "X"
      
      setNames(x, new_nms)
    },
    
    # fast utility to get nr of lines in a file
    count_lines = function(fn){
      R.utils::countLines(fn)
    },
    
    checksum = function(fn){
      tools::md5sum(fn)  
    },
    
    compare_n_rows = function(fn, data){
      
      list(n_lines = private$count_lines(fn),
           n_rows = nrow(data))
      
    },
    
    existing_columns = function(x, data){
      
      nm <- names(data)
      if(all(x %in% nm)){
        return(x)
      } else {
        #pm_log("Columns not in data: {setdiff(x,nm)}")
        return(intersect(x, nm))
      }
      
    }
    
    
  )
)
