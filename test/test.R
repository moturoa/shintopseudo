
library(glue)
library(yaml)
library(futile.logger)
library(dplyr)
library(DBI)
library(RSQLite)
library(dbplyr)
library(data.table)
library(lubridate)
library(safer)
library(readr)

#cfg_path  <- "test/config_ssd.yml"
 #cfg_path  <- "test/config_instadr.yml"
# cfg_path  <- "test/config_izm.yml"
#cfg_path  <- "test/config_openwave.yml"
cfg_path <- "test/config_brp_IZM_short.yml"


devtools::load_all()

.pdb <- pseudoDB$new(cfg_path, secret = "banaan",
                     max_n_lines = 1000)


.pdb$process_files()

.pdb$close_sqlite()


.pdb <- pseudoDB$new(cfg_path, secret = "banaan")

con <- dbConnect(RSQLite::SQLite(), "c:/repos/ede/DATA/izm/q/shinto_pseudomaker.sqlite")

dat <- dbReadTable(con, "datadienst")


.pdb$decrypt(dat$value)


key <- dbReadTable(con, "keystore")
key



# tests

.pdb$files
.pdb$project

.pdb$config[.pdb$files[1]]

.pdb$decrypt(.pdb$encrypt(c("hallo","daar")))

.pdb$encrypt(c("hallo","daar")) %>%
  .pdb$decrypt(.)


data <- lapply(.pdb$files, .pdb$read_data)
.pdb$datalog


data1 <- .pdb$read_data(.pdb$files[1])
data2 <- .pdb$anonymize_column(data1, column = names(.pdb$config[[1]]$config$encrypt)[1])


data3 <- .pdb$anonymize_columns(data1, 
                                columns = names(.pdb$config[[1]]$config$encrypt),
                                db_keys = unlist(.pdb$config[[1]]$config$encrypt)
                                )




d <- read.csv2("c:/repos/ede/DATA/datadienst/test-output/institutionele_adressen/adressen_lijst.csv")
x <- .pdb$decrypt(d$PC)

