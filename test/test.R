
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

cfg_path  <- "test/config_ssd.yml"



devtools::load_all()

.pdb <- pseudoDB$new(cfg_path, secret = "banaan")

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

.pdb$process_files()


.pdb$close_sqlite()


