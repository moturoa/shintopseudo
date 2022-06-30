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
pseudoDB <- R6::R6Class(
  public = list(
    
    con = NULL,
    project = NULL,
    config = NULL,
    files = NULL,
    dbtable = NULL,
    datalog = NULL,
    
    initialize = function(config_file, secret, log_to = c("file","stdout")){
      
      log_to <- match.arg(log_to)
      
      cfg <- self$read_config(config_file)
      self$config <- cfg$config
      self$project <- cfg$project
      
      self$files <- names(self$config)
      
      self$check_files_exist()
      
      private$secret <- secret
      
      self$create_directories()
      
      if(log_to == "file"){
        self$open_logfile()
      }
      
      self$dbtable <- "datadienst"
      self$con <- self$open_sqlite()
      
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
        encrypted_columns = self$encrypted_columns()
      )
      
    },
    
    create_directories = function(){
      dir.create(self$project$outputdir, showWarnings = FALSE)
      dir.create(self$project$database, showWarnings = FALSE)
      dir.create(self$project$logdir, showWarnings = FALSE)
    },
    
    open_logfile = function(){
      
      today_ <- format(Sys.Date(), "%Y%m%d")
      fn <- file.path(self$project$logdir, 
                      paste0(today_, "_pseudomaker.log"))
      
      futile.logger::flog.appender(futile.logger::appender.tee(fn), name = "pseudomaker")
      self$log("------------- start pseudoDB v. {packageVersion('shintopseudo')} -------------")

    },
    
    write_datalog = function(){
      
      path <- file.path(self$project$outputdir, 
                        "shintopseudo.csv")
                     
      write.csv2(self$datalog, path, row.names = FALSE)
      
    },
    
    
    log = function(msg, how = c("info","fatal","warn")){
      
      how <- match.arg(how)
        
      msg <- glue::glue(msg, .envir = parent.frame(n = 1))
      switch(how, 
             fatal = futile.logger::flog.fatal(msg, name = "pseudomaker"),
             info = futile.logger::flog.info(msg, name = "pseudomaker"),
             warn = futile.logger::flog.warn(msg, name = "pseudomaker"))
      
    },
    
    set_data_log = function(file, what, value){

      self$datalog[[what]][self$datalog$file == file] <- value
      
    },
    
    set_status = function(file, status){
      self$set_data_log(file, "status", status)
    },
    
    set_error = function(file, error){
      self$set_status(file, "FAIL")
      self$set_data_log(file, "timestamp", as.character(Sys.time()))
      self$set_data_log(file, "error_code", error)
    },
    
    read_config = function(fn){
      
      out <- try(yaml::read_yaml(fn), silent = TRUE)
      
      if(inherits(out, "try-error")){
        self$log("Error reading config ({fn})", "fatal")
        return(FALSE)
      }
      
      for(i in seq_along(out$config)){
        out$config[[i]]$name <- names(out$config)[i]
      }
      
      if(is.null(out$project$clean)){
        out$project$clean <- FALSE
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
        self$log("Some files not found: {paste(nonex, collapse = ', ')}", "warn")
      }
    },
    
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
    
    vacuum_sqlite = function(){
        
      out <- try(DBI::dbExecute(self$con, "vacuum;"))
      if(inherits(out, "try-error") | out[1] != 0){
        self$log("VACUUM command not successful!", "fatal")
      } else {
        self$log("Database vacuum complete")
      }
      
    },
    
    close_sqlite = function(vacuum = TRUE){
      if(vacuum){
        self$vacuum_sqlite()
      }
      DBI::dbDisconnect(self$con)
      self$log("Database connection terminated.")
    },
    
    close = function(){
      
      self$write_datalog()
      self$close_sqlite()
      self$log("------------- end pseudoDB -------------")
      
    },
    
    
    #' @param fn Bare filename to read (full path is read from config)
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
      
      if(is.null(cfg$readfunction)){
        tm <- system.time({
          out <- self$read_data_fread(fn_path, 
                                      quote = "\"",
                                      sep = cfg$`csv-separator`, 
                                      fill = TRUE,
                                      skip = skip,
                                      encoding = cfg$encoding)  
        })  
      } else {
        
        if(!exists(cfg$readfunction)){
          self$set_error(fn, paste("readfunction",cfg$readfunction,"not found."))
          return(NULL)
        } else {
          read_fun <- base::get(cfg$readfunction)
          tm <- system.time(
            out <- read_fun(fn_path)
          )
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
    
    
    
    read_data_fread = function(fn,
                               quote,
                               sep,
                               fill, 
                               skip = 0,
                               encoding = NULL){
      
      if(is.null(encoding) || encoding == ""){
        encoding <- "unknown"
      }
      
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
    
    
    symmetric_encrypt_columns = function(data, columns, new_names = NULL){
    
      for(i in seq_along(columns)){
        
        out_column <- ifelse(is.null(new_names[i]), columns[i], new_names[i])
        
        data[[out_column]] <- self$encrypt(data[[columns[i]]])  
        
        self$log("{nrow(data)} values symmetric encrypted, column: {column} to {out_column}") 
        
      }  
      
      
      data  
    },
    
    
    anonymize_column = function(data, column, db_key = NULL,
                                store_key_columns = NULL,
                                file = NULL){
      
      st <- proc.time()[3]
      
      # db_key is for looking up encrypted value/hash pairs from the db.
      # if not provided, use column name itself.
      if(is.null(db_key) || db_key == ""){
        self$log("No key set for {column}, using column name as key - safer to set a key!", "warn")
        db_key <- column
      }
      
      # value/hash pairs
      u <- unique(data[[column]])
      
      #! next 2 steps to pm_normalize
      # remove spaces (anywhere)
      u <- unique(gsub("[[:space:]]", "", u))
      
      # remove values we don't want to hash.
      u <- u[!is.na(u) & u != ""]
      
      # key/value/hash table (will refill with already hashed values below)
      key <- tibble(
        key = db_key,
        value = u,
        hash = private$make_hash(length(u))
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
        
        # Set encoding to UTF8
        key_store <- as_tibble(lapply(key_store, function(x){
          iconv(x, from = "latin1", to = "UTF-8", sub = "byte")
        }))
        
        dbWriteTable(self$con, "keystore", key_store, overwrite = TRUE)
        
        self$log(glue("Key columns stored ({nrow(key_store)} rows)"))
      }
      
      # Append value/hash to DB (only those rows not previously stored).
      if(nrow(key_db_new) > 0){
        dbWriteTable(self$con, self$dbtable, key_db_new, append = TRUE)
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
    
    
    anonymize_columns = function(data, columns, db_keys, file, ...){
      
      for(i in seq_along(columns)){
        data <- self$anonymize_column(data, columns[i], db_key = db_keys[i], file=file, ...)
      }
      
      return(data)
    },

    
    
    
    
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
        
        if(nrow(out) < 3){
          self$set_error(fn, "File is (nearly) empty")
          next
        }
        
        if(!all(names(cfg$encrypt) %in% names(out))){
          
          nm_mis <- setdiff(names(out), names(cfg$encrypt))
          
          self$log("Columns not found: {paste(nm_mis, collapse=',')}.", "fatal")
          self$log("Available columns: {paste(names(out), collapse = ',')}", "fatal")
          self$set_error(fn, "Column(s) not found in data")
          next
        }
        
        out <- out %>%
          self$anonymize_columns(columns = names(cfg$encrypt),
                            db_keys = unlist(cfg$encrypt),
                            store_key_columns = self$config[[fn]]$store_key_columns,
                            file = fn) %>%
          self$symmetric_encrypt_columns(columns = names(cfg$symmetric),
                                         new_names = unlist(cfg$symmetric)) %>%
          self$delete_columns(cfg$remove) %>%
          self$keep_columns(cfg$keep) %>%
          self$date_to_year(cfg$date_to_year) %>%
          self$to_age_bracket(cfg$to_age_bracket)

        self$write_data(out, fn)
        chk <- private$checksum(file.path(self$project$outputdir, fn))
        self$set_data_log(fn, "md5_out", chk)
        self$set_data_log(fn, "timestamp", as.character(Sys.time()))
        self$set_status(fn, "OK")
        
        
      }

    },
    
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
    
    
    to_age_bracket = function(data, columns){
      
      if(length(columns) == 0)return(data)
      
      brks <- seq(0,150,by=5)
      n <- length(brks)
      labs <- paste(brks[1:(n-1)], "-", brks[2:n])
      
      for(i in seq_along(columns)){
        data[[columns[i]]] <- as.character(cut(data[[columns[i]]], 
                                               breaks = brks,
                                               labels = labs))
      }
      
      data
    },
    
    keep_columns = function(data, columns){
      if(length(columns) == 0)return(data)
      columns <- self$existing_columns(columns, data)
      dplyr::select(data, dplyr::all_of(columns))
    },
    
    
    delete_columns = function(data, columns){
      if(length(columns) == 0)return(data)
      columns <- self$existing_columns(columns, data)
      dplyr::select(data, -dplyr::all_of(columns))
    },
    
    
    existing_columns = function(x, data){
      
      nm <- names(data)
      if(all(x %in% nm)){
        return(x)
      } else {
        #pm_log("Columns not in data: {setdiff(x,nm)}")
        return(intersect(x, nm))
      }
      
    },
    
    encrypted_columns = function(){
        unname(sapply(lapply(lapply(self$config, "[[", "config"), "[[", "encrypt"), jsonlite::toJSON))  
    }
    
  ),
  
  private = list(
    
    secret = NULL,
    
    make_hash = function(n = 1, n_phrase = 9){
      txt <- c(letters,LETTERS,0:n_phrase)
      replicate(n,paste(sample(txt, n_phrase), collapse=""))
    },
    
    fix_names = function(x){
      
      new_nms <- gsub("\"","", names(x))
      new_nms[new_nms == ''] <- "X"
      
      setNames(x, new_nms)
    },
    
    count_lines = function(fn){
      R.utils::countLines(fn)
    },
    
    checksum = function(fn){
      tools::md5sum(fn)  
    },
    
    compare_n_rows = function(fn, data){
      
      list(n_lines = private$count_lines(fn),
           n_rows = nrow(data))
      
    }
    
    
  )
)
