
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
