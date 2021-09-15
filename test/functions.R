



pm_read_config <- function(fn){
  
  out <- try(yaml::read_yaml(fn))
  
  if(inherits(out, "try-error")){
    pm_log("Error reading config ({fn})", "fatal")
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

# functions
pm_open_sqlite <- function(.cc){
  
  path <- .cc$project$database
  dbname <- .cc$project$databasename
  fn <- file.path(path, dbname)
  db_try <- try(DBI::dbConnect(RSQLite::SQLite(), fn))
  
  if(inherits(db_try, "try-error")){
    pm_log("Cannot open SQLite database on path {path}", "fatal")
  } else {
    pm_log("Opened SQLite database ({dbname}) on path {path}")
    return(db_try)
  }
  
}


pm_empty_sqlite <- function(db, areyousure=FALSE){
  if(areyousure){
    dbWriteTable(db, .dbtable,
                 tibble(
                   key = character(0),
                   value = character(0),
                   hash = character(0)
                 ),
                 overwrite = TRUE)
  }
}


pm_vacuum_db <- function(con){
  
  out <- try(dbExecute(con, "vacuum;"))
  if(inherits(out, "try-error") | out[1] != 0){
    pm_log("VACUUM command not successful!", "fatal")
  } else {
    pm_log("Database vacuum complete")
  }
}

pm_close_db <- function(con){
  
  dbDisconnect(con)
  flog.info("SQLite connection closed.", name = "pseudomaker")
}

pm_open_logfile <- function(path){
  
  today_ <- format(Sys.Date(), "%Y%m%d")
  fn <- file.path(path, paste0(today_, "_pseudomaker.log"))
  
  flog.appender(appender.tee(fn), name = "pseudomaker")
  pm_log("------------- start pseudomaker v. {.version} -------------")
}


pm_log <- function(msg, how = c("info","fatal","warn")){
  how <- match.arg(how)
  
  msg <- glue(msg, .envir = parent.frame(n = 1))
  switch(how, 
         fatal = flog.fatal(msg, name = "pseudomaker"),
         info = flog.info(msg, name = "pseudomaker"),
         warn = flog.warn(msg, name = "pseudomaker"))
}


# Ede
pm_fwf_read <- function(fn, widths){
  
  nms <- names(read.table(fn, nrows = 1, header = TRUE, check.names = FALSE))
  out <- read.fwf(fn, widths = widths, header = FALSE, 
           skip = 4,
           comment.char="",
           colClasses = "character") %>%
    setNames(nms)
  as_tibble(lapply(out, trimws))
  
}



pm_csv_read <- function(fn, sep = ";", 
                        encoding = NULL,
                        method = 1, 
                        skip_lines = NULL,
                        scrub_quotes = FALSE){
  
  if(is.null(sep))sep <- "auto"
  if(is.null(encoding) || encoding == "")encoding <- "unknown"
  if(is.null(method))method <- 1
  
  if(is.null(skip_lines))skip_lines <- "__auto__"
  
  n_lines_file <- R.utils::countLines(fn)
  
  if(method %in% c(1,2,5)){
      
    dt_quote <- switch(as.character(method),
                       "1" = "",
                       "2" = "\"",
                       "5" = "")
    
    dt_fill <- switch(as.character(method),
                      "1" = FALSE, 
                      "2" = TRUE,
                      "5" = TRUE)
    
    if(method == 5)skip_lines <- 2
      
    tm <- system.time({
      
        out <- try(data.table::fread(fn, 
                                     quote = dt_quote,
                                     sep = sep, 
                                     fill = dt_fill,
                                     skip = skip_lines,
                                     encoding = encoding,
                                     showProgress = FALSE,
                                     colClasses = "character"),
                   silent = TRUE)
    })
    
    if(inherits(out, "try-error")){
      
      dt_error <- as.character(out)
      
      pm_log("data.table::fread failed, trying read.table")
      out <- suppressWarnings(try(read.table(fn, 
                                             sep = sep, 
                                             header = TRUE,
                                             encoding = encoding,
                                             check.names = FALSE,
                                             colClasses = "character"),
                 silent = TRUE))
      
    }
    if(inherits(out, "try-error")){
      
      rt_error <- as.character(out)
      pm_log("File {fn} could not be read!", "fatal")
      pm_log("data.table::fread ERROR: {dt_error}", "fatal")
      pm_log("read.table ERROR: {rt_error}", "fatal")
      stop("STOP")
      
    }
  } else if(method == 3){
    
    
    own_readtable <- function(fn){
      
      r <- readLines(fn, encoding = "UTF-8", skipNul = TRUE, warn = TRUE)
      
      nms <- strsplit(r[1], ";")[[1]]
      l <- strsplit(r[2:length(r)], ";")
      n <- sapply(l, length)
      
      
      d8 <- l[n == 8] # lijnen zonder security comment
      d8 <- do.call(rbind, d8)
      colnames(d8) <- nms[1:8]
      d8 <- as_tibble(d8)
      
      d9 <- l[n == 9] # lijnen met (sommige vallen op de volgende lijn, die nemen we niet mee)
      d9 <- do.call(rbind, d9)
      colnames(d9) <- nms
      d9 <- as_tibble(d9)
      
      data <- bind_rows(d8, d9)
      
      return(data)
      
    }
    
    n_lines <- length(readLines(fn))
    pm_log("readLines nr of lines : {n_lines}")
    
    tm <- system.time({
      out <- try(own_readtable(fn), silent = FALSE)
    })
    
  } else if(method == 4){
    
    tm <- system.time({
      out <- try(read.table(fn, 
                                           sep = sep, 
                                           fill = TRUE,
                                           header = TRUE,
                                           encoding = encoding,
                                           check.names = FALSE,
                                           colClasses = "character"),
                                  silent = TRUE)
    })
    
    
    if(inherits(out, "try-error")){
      
      rt_error <- as.character(out)
      pm_log("File {fn} could not be read!", "fatal")
      pm_log("read.table ERROR: {rt_error}", "fatal")
      stop("STOP")
      
    }
    
  } else if(method == 5){
    
    # specifiek voor allegro (voor nu)
    tm <- system.time({
      out <- suppressWarnings(try(
               pm_fwf_read(fn, widths = c(12, 16, 18, 18, 11, 10)),
               silent = TRUE))
    })
    
    
    if(inherits(out, "try-error")){
      
      rt_error <- as.character(out)
      pm_log("File {fn} could not be read!", "fatal")
      pm_log("read.fwf ERROR: {rt_error}", "fatal")
      stop("STOP")
      
    }
  } else if(method == 6){
    
    tm <- system.time({
      out <- try(read.table(fn, 
                            sep = sep, 
                            fill = TRUE,
                            header = TRUE,
                            quote = "",
                            encoding = encoding,
                            check.names = FALSE,
                            colClasses = "character"),
                 silent = TRUE)
    })
    
    
    if(inherits(out, "try-error")){
      
      rt_error <- as.character(out)
      pm_log("File {fn} could not be read!", "fatal")
      pm_log("read.table ERROR: {rt_error}", "fatal")
      stop("STOP")
      
    } 
    
    # speciaal voor linkit :)
    names(out) <- gsub("\"", "", names(out))
    names(out)[names(out) == ''] <- "X"

  }
  

  # remove \"
  if(isTRUE(scrub_quotes)){
    out <- as_tibble(lapply(out, function(x)gsub("\"", "", x)))  
  }
  
  mem <- round(object.size(out) * 1E-06, 1)
  pm_log("'{basename(fn)}' ({nrow(out)} lines) read in {round(tm[3],1)} sec., occupying {mem}MB memory.")

  if(!is.data.frame(out) || is.null(out) || nrow(out) == 0){
    pm_log("data malformed in some way or empty, exiting.", "fatal")
    stop("bad data.")
  }
  
  n_lines_data <- nrow(out) + 1
  if(n_lines_data != n_lines_file){
    pm_log(paste0("Not all lines read from data: file has {n_lines_file} lines,",
           "data has {n_lines_data} lines"), "warn")
  }
  
return(pm_fix_names(out))
}


pm_check_files_exist <- function(conf){
  
  files <- names(conf$config)
  fns <- file.path(conf$project$inputdir, files)
  
  ex <- file.exists(fns)
  if(!all(ex)){
    nonex <- fns[!ex]
    pm_log("Files not found: {paste(nonex, collapse = ', ')}", "fatal")
    stop("Some files not found! Check logs.")
  }
}


pm_csv_write <- function(data, fn){
  
  # avoid list column problem
  data <- as_tibble(apply(data, 2, as.character))
  
  tm <- system.time(
    data.table::fwrite(data, fn, sep = ";")  
  )
  
  pm_log("'{basename(fn)}' ({nrow(data)} lines) written in {round(tm[3],1)} sec.")
  
}

pm_rds_write <- function(data, fn){
  
  pth <- tools::file_path_sans_ext(fn)
  fn_out <- paste0(pth, ".rds")
  
  saveRDS(data, fn_out)
  
  pm_log("'{basename(fn_out)}' (R binary) written.")
  
}


pm_fix_names <- function(x){
  setNames(x, gsub("\"","", names(x)))
}


make_hash <- function(n = 1, n_phrase = 9){
  txt <- c(letters,LETTERS,0:9)
  replicate(n,paste(sample(txt, n_phrase), collapse=""))
}





date_to_year <- function(data, column){
  
  for(i in seq_along(column)){
    dts <- as.Date(data[[column[i]]], format = "%d-%m-%Y")
    if(all(is.na(dts))){
      data[[column[i]]] <- substr(data[[column[i]]], 1, 4)
    } else {
      data[[column[i]]] <- as.character(year(dts))  
    }
    
  }
  
  data
}


to_age_bracket <- function(data, columns){
  
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
}


existing_columns <- function(x, data){
  
  nm <- names(data)
  if(all(x %in% nm)){
    return(x)
  } else {
    pm_log("Columns not in data: {setdiff(x,nm)}")
    return(intersect(x, nm))
  }
  
}

keep_columns <- function(data, columns){
  if(length(columns) == 0)return(data)
  columns <- existing_columns(columns, data)
  dplyr::select(data, all_of(columns))
}


delete_columns <- function(data, columns){
  if(length(columns) == 0)return(data)
  columns <- existing_columns(columns, data)
  dplyr::select(data, -all_of(columns))
}

pm_encrypt <- function(x){
  
  vapply(x, safer::encrypt_string, key = .secret,
         USE.NAMES = FALSE, FUN.VALUE = character(1)
         )
}

pm_decrypt <- function(x){
  
  vapply(x, safer::decrypt_string, key = .secret,
         USE.NAMES = FALSE, FUN.VALUE = character(1)
  )
}





scramble_columns <- function(data, columns, db_keys, ...){
  
  for(i in seq_along(columns)){
    data <- scramble_column(data, columns[i], db_key = db_keys[i], ...)
  }
  
return(data)
}





scramble_column <- function(data, column = NULL, db = NULL, db_key = NULL, 
                            store_key_columns = NULL){
  
  if(is.null(column) || length(column) == 0){
    return(data)
  }
  
  st <- proc.time()[3]
  
  if(!column %in% names(data)){
    pm_log("Column {column} not found in data - exiting.", "fatal")
    pm_log("Available columns: {paste(names(data), collapse = ', ')}", "fatal")
    stop("Column not found.")
  }
  
  # db_key is for looking up encrypted value/hash pairs from the db.
  # if not provided, use column name itself.
  if(is.null(db_key) | db_key == ""){
    pm_log("No key set for {column}, using column name as key - safer to set a key!", "warn")
    db_key <- column
  }
  
  # value/hash pairs
  u <- unique(data[[column]])
  
  #! next 2 steps to pm_normalize
  # remove spaces (anywhere)
  u <- unique(gsub("[[:space:]]", "", u))
  
  # remove values we don't want to hash.
  u <- u[!is.na(u) & u != ""]
  
  key <- tibble(
    key = db_key,
    value = u,
    hash = make_hash(length(u))
  )
  
  # encrypt
  key$value <- pm_encrypt(key$value)
  
  # First use: table does not exist.
  if(!.dbtable %in% dbListTables(db)){
    
    db_tab <- tibble(
      key = character(0),  # varchar
      value = character(0),
      hash = character(0)
    )  
    dbWriteTable(db, .dbtable, db_tab)
  }

  # Read previous hash/value pairs for this db_key.
  key_db <- tbl(db, .dbtable) %>%
    filter(key == {{db_key}})
  
  key_db_overlap <- try(
    filter(key_db, value %in% !!key$value) %>%
      collect
  )
  
  if(inherits(key_db_overlap, "try-error")){
    pm_log("Could not read from SQLite.", "fatal")
    stop("Could not read from SQLite.")
  }
  
  if(nrow(key_db_overlap)){
    pm_log("{nrow(key_db_overlap)} value/hashes read from SQLite.") 
  }
  
  # New hash/value pairs not previously encrypted.
  key_db_new <- filter(key, !value %in% !!key_db_overlap$value)
  
  # Now adjust the key with the db hash/value pairs, if any.
  if(nrow(key_db_overlap) > 0){
    i_m <- match(key$value, key_db_overlap$value)
    if(length(i_m)){
      
      isn <- which(!is.na(i_m))
      
      key$hash[isn] <- key_db_overlap$hash[i_m[isn]]
      
    }  
  }
  
  
  # store_key_columns: alleen aanpassen op basis van 1e entry.
  if(!is.null(store_key_columns) && column == store_key_columns[1]){
    
    if(!all(store_key_columns %in% names(data))){
      pm_log("Not all store_key_columns found in data, skipping saving.", "warn")
    }
    
    db_key_col <- names(db_key)
    if(!db_key_col %in% store_key_columns){
      store_key_columns <- c(db_key_col, store_key_columns)
    }
    
    vals <- dplyr::select(data, all_of(store_key_columns)) %>%
      dplyr::filter(!is.na(!!sym(db_key_col)))
    
    key_store <- key
    key_store$value <- pm_decrypt(key_store$value)
    
    key_store <- key_store %>% 
      left_join(vals, by = c(value = db_key_col)) %>%
      rename(!!db_key_col := value) %>%
      dplyr::select(-key)
    
    key_store <- as_tibble(lapply(key_store, function(x){
      iconv(x, from = "latin1", to = "UTF-8", sub = "byte")
    }))
    
    dbWriteTable(db, "keystore", key_store, overwrite = TRUE)
  }
  
    
  # Write value/hash to (only those rows not previously stored).
  if(nrow(key_db_new) > 0){
    dbWriteTable(db, .dbtable, key_db_new, append = TRUE)
    pm_log("{nrow(key_db_new)} value/hashes written to SQLite.")
  }
  
  # Finally assign hashed values.
  key_value <- pm_decrypt(key$value)
  
  # should not be necessary, but make sure we have no NA or ""
  key_value <- gsub("[[:space:]]", "", key_value)
  key_value <- key_value[!is.na(key_value) & key_value != ""]
  
  # before looking up, also remove spaces.
  # (move this to a normalize function!)
  data_column <- gsub("[[:space:]]", "", data[[column]])
  
  hashed <- key$hash[match(data_column, key_value)]
  data[[column]] <- hashed
  
  # Logging
  nd <- proc.time()[3]
  pm_log("{nrow(data)} values hashed ({column}) in {round(nd-st,1)} sec.")
  
  return(data)
}


pm_open_semafoor <- function(){
  fn <- file.path(.cc$project$outputdir, "locked.sem")
  writeLines("hello from shintolabs", fn)
}

pm_close_semafoor <- function(){
  fn <- file.path(.cc$project$outputdir, "locked.sem")
  unlink(fn)  
}


pm_process_files <- function(.cc){
  
  metadat <- list()
  
  root_dir <- getOption("pm_root_dir", "")
  
  for(i in seq_along(.cc$config)){
    
    fconf <- .cc$config[[i]]
    
    # read from config
    fn <- file.path(.cc$project$inputdir, fconf$name)
    
    if(root_dir != ""){
      fn <- file.path(root_dir, fn)
    }
    
    data <- pm_csv_read(fn, 
                        sep = fconf[["csv-separator"]], 
                        encoding = fconf[["encoding"]],
                        method = fconf$readmethod,
                        scrub_quotes = fconf$scrub_quotes) %>%
      scramble_columns(columns = names(fconf$config$encrypt),
                       db = db, 
                       db_keys = unlist(fconf$config$encrypt),
                       store_key_columns = fconf$store_key_columns
                       ) %>%
      delete_columns(fconf$config$remove) %>%
      keep_columns(fconf$config$keep) %>%
      date_to_year(fconf$config$date_to_year) %>%
      to_age_bracket(fconf$config$to_age_bracket)
    
    # write data
    out_file <- file.path(.cc$project$outputdir, basename(fn))
    pm_csv_write(data,  out_file)
    
    if(.cc$project$saverds){
      pm_rds_write(data,  out_file)  
    }
    
    #!! naar functie
    metadat[[i]] <- tibble(
      filename = out_file,
      md5 = tools::md5sum(out_file),
      timestamp = Sys.time()
    )
    
    # clean up
    if(.cc$project$clean){
      unlink(fn)
    }
    
  }
  
  metadat <- dplyr::bind_rows(metadat)
  write.table(metadat, 
              file.path(.cc$project$outputdir, "pseudomaker.info"),
              sep = ";", col.names = TRUE, row.names = FALSE)
  
}

