

validate_address <- function(data, adres_column, bag, 
                             bag_columns = c("adresseerbaarobject_id",
                                             "openbareruimtenaam",
                                             "huisnummer","huisletter",
                                             "huisnummertoevoeging")){
  
  if(!adres_column %in% names(data)){
    stop(paste("Column",adres_column,"not found."))
  }
  
  adres <- unique(data[[adres_column]])
  adres <- adres[!is.na(adres)]
  
  # make UTF8
  adres <- iconv(adres, to = "UTF-8")
  #adres <- stringi::stri_enc_toutf8(adres)
  
  adres_out <- cbind(adres, split_adres_field(adres, bag))
  names(adres_out)[1] <- adres_column
  
  adres_out <- merge_bag_columns(adres_out, bag, bag_columns) %>%
    dplyr::distinct(!!sym(adres_column), .keep_all = TRUE)  # soms dubbele match
  
  adres_out <- adres_out[,c(adres_column, bag_columns)]
  
  out <- dplyr::left_join(data, adres_out, by = adres_column)
  
  if(nrow(out) != nrow(data)){
    stop("Probleem in validate_address, aantal rijen output niet gelijk aan input!")
  }
  
  out 
}





merge_bag_columns <- function(data, bag, bag_columns = "adresseerbaarobject_id"){
  
  adres_columns <-  c("openbareruimtenaam","huisnummer","huisletter","huisnummertoevoeging")

  # nog ca. 100 panden met meer dan 1 rij per bag_adres.
  # deze hebben meerdere pandid per adres (geen idee waarom).
  bag <- distinct(bag, !!sym(bag_columns[1]), .keep_all = TRUE) %>%
    mutate(huisnummer = as.character(huisnummer), # was vroeger char
           huisletter = tidyr::replace_na(huisletter, ""),
           huisnummer = tidyr::replace_na(huisnummer, ""),
           huisnummertoevoeging = tidyr::replace_na(huisnummertoevoeging, ""))
  
  bag_columns <- union(adres_columns, bag_columns)
  bag <- dplyr::select(bag,dplyr::all_of(bag_columns))
  
  dplyr::left_join(data, bag, by = adres_columns)
}




#---- Utils

# add space between number and letter in huisnummer ("12A" --> "12 A")
space_huisnummer_huisletter <- function(x){
  
  gsub("([0-9])([A-Z])", "\\1 \\2", x, perl = TRUE)
  
}

# gsub("([0-9])([A-Z])", "\\1 \\2", "Huisstraat 12A", perl = TRUE)
# gsub("([0-9])([A-Z])", "\\1 \\2", "12A", perl = TRUE)
# gsub("([0-9])([A-Z])", "\\1 \\2", "12A 220", perl = TRUE)
# gsub("([0-9])([A-Z])", "\\1 \\2", "12 A", perl = TRUE)
#grep("([0-9])([A-Z])", "12A")
#gsub("([0-9])([A-Z])", "\\1 \\2", "12A 220", perl = TRUE)



#adres_huisnummer_etc("1e Lieven de Keylaan 20A 10")
adres_huisnummer_etc <- function(x){
  out <- strsplit(x, "[a-z|A-Z]{2,}")  
  
  vapply(out, function(x)x[length(x)], FUN.VALUE = character(1)) %>% 
    stringr::str_trim(.)
}



adres_openbareruimtenaam <- function(x){
  
  out <- vapply(x, function(txt){
    
    tryCatch(stringi::stri_replace_last_fixed(txt, adres_huisnummer_etc(txt), ""),
             error = function(e)NA_character_,
             warning = function(w)"")
    
  }, FUN.VALUE = character(1))
  
  
  stringr::str_trim(out)
}

# 
# adres_huisnummer_etc("Huisstraat 12A") %>% 
#   space_huisnummer_huisletter()





find_abbr <- function(x, m){
  if(x == "")return(NA_character_)
  
  x <- gsub("[(]|[)]|[*]|[+]|[?]", " ", x)
  
  y <- strsplit(x, "")[[1]]
  
  reg <- paste(paste0(".*",y),collapse="")
  out <- try(grep(reg, m, value = TRUE))
  #if(inherits(out, "try-error"))browser()
  if(length(out) == 0){
    return(NA_character_)
  } else {
    return(out)
  }
}

#s <- sapply(straat_mis, function(x)find_abbr(x,unique(bag_eindhoven$openbareruimtenaam) ))




fix_openbareruimtenaam <- function(x, bag){
  
  
  u <- unique(x[!x %in% bag$openbareruimtenaam])
  
  out <- fix_openbareruimtenaam_v(u, bag)
  
  lookup <- tibble(value = u, fixed = out)
  
  m <- match(x, lookup$value)
  fix <- lookup$fixed[m]
  x[!is.na(fix)] <- fix[!is.na(fix)]
  
  return(x)
}


fix_openbareruimtenaam_v <- function(x, bag){
  
  out <- x
  b_s <- unique(bag$openbareruimtenaam)
  
  for(i in seq_along(x)){
    
    if(!x[i] %in% b_s){
      
      if(x[i] == ""){
        next
      }
      
      # de straat is een afkorting
      a <- find_abbr(x[i], b_s)
      if(!all(is.na(a))){
        if(length(a) == 1){
          out[i] <- a
          next  
        }
      }
      
      # Straat zoeken op approx. grep.
      g <- agrep(x[i], b_s, max.distance = 1, value = TRUE)
      if(length(g) > 0){  # minstens 1 resultaat
        
        # Meer dan 1 match:
        if(length(g) > 1){
          nc <- nchar(g)
          nx <- nchar(x[i])
          
          # Kies resultaat met zelfde lengte, dit is vaak enkele
          # substituut (bv. Aida --> Aïda)
          if(any(nc == nx)){
            g <- g[which(nc == nx)[1]]
          } else {
            
            # Meerdere resultaten, pak de eerste.
            # Hopelijk gaat dit vaak goed :)
            g <- g[1]
          }
          
        }
        out[i] <- g
      }
      
    }
    
    
  } 
  
  out
  
}


split_adres_field <- function(x, bag){
  
  hh <- adres_huisnummer_etc(x) %>% 
    space_huisnummer_huisletter()
  
  h <- strsplit(hh, " ")
  
  is_txt <- function(x){
    grepl("[a-z]", x, ignore.case = TRUE)
  }
  
  out <- cbind(tibble(openbareruimtenaam = adres_openbareruimtenaam(x)),
               as_tibble(
                 do.call(rbind, lapply(h, function(l){
                   c(huisnummer = l[1],
                     huisletter = if(is_txt(l[2]))l[2] else NA_character_,
                     huisnummertoevoeging = if(is_txt(l[2]))l[3] else l[2])
                 })))
  )
  
  # fix huisletter = "R-500"
  i_fix <- grep("-", out$huisletter)
  fix_lis <- strsplit(out[i_fix,"huisletter"],"-")
  
  # huisletter in eerste veld
  # TODO als dit geen letter is, moet het een huisnummertoevoeging zijn?
  out$huisletter[i_fix] <- sapply(fix_lis, "[[", 1)
  
  # huisnummertoevoeging in tweede
  i_twofields <- which(sapply(fix_lis, length) == 2)
  if(length(i_twofields)){
    out$huisnummertoevoeging[i_fix[i_twofields]] <- sapply(fix_lis[i_twofields], "[[", 2)  
  }
  
  out[is.na(out)] <- ""
  
  out$openbareruimtenaam <- fix_openbareruimtenaam(out$openbareruimtenaam, bag)
  
  out$huisnummertoevoeging[out$huisnummertoevoeging == "-"] <- ""
  out$huisnummertoevoeging[nchar(out$huisnummertoevoeging) > 6] <- ""
  
  out
}



# split_adres_field("Nuenenseweg 1", bag_eindhoven)
# split_adres_field("Nuenenseweg 1A", bag_eindhoven)
# split_adres_field("Nuenenseweg 1 A", bag_eindhoven)
# split_adres_field("Nuenenseweg 1 67", bag_eindhoven)
# split_adres_field("Nuenenseweg 1A 67", bag_eindhoven)
# split_adres_field("Nuenenseweg 1 A 67", bag_eindhoven)



