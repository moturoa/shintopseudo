# (c)2020 Shinto Labs B.V. - All Rights Reserved.
# NOTICE: All information, intellectual and technical concepta contained herein is, and remains
# the property of Shinto Labs B.V.

#----- pseudomaker -----#

#---- Command line arguments ----
args <- commandArgs(trailingOnly = TRUE)
c_file <- args[1]

.version <- "1.3-2"

# gaat naar environment variable, oid
# alleen gebruikt om values in de hash/value tables te encrypten/decrypten,
# niet om hashes aan te maken (die zijn random)
.secret <- "55d79c1ecc42cc8b3cac657afe96f098"


#----- Packages, functions -----
source("R/load_packages.R")
source("R/functions.R")


#----- Read config -----
.cc <- pm_read_config(c_file)
  
#----- Semafoor ----
pm_open_semafoor()


#----- Logging ------
pm_open_logfile(.cc$project$logdir)



#----- Open SQLite db -----
db <- pm_open_sqlite(.cc)
.dbtable <- "datadienst"  # hardcoded voorlopig


#----- File exists check ------
pm_check_files_exist(.cc)


#----- Start encryption -----
pm_process_files(.cc)

#----- Clean up -----
pm_vacuum_db(db)
pm_close_db(db)


flog.info("------------- end pseudomaker -------------", name = "pseudomaker")
pm_close_semafoor()

