
# Dependencies for testing
#library(shintopseudo)
devtools::load_all()
library(dplyr)
library(shintodb)

# Initialize the object with a config file, and a secret (for symmetric encryption of value column in the datadienst table)
.pdb <- pseudoDB$new("test/config_brp_test.yml", secret = "banaan")

# Gives a warning if some files don't exist (process never crashes when a file is not found or corrupt)
.pdb$check_files_exist()

# Run the encryption/other transformations
# - makes output files
# - makes *.log in the log output folder (old logs), and 'shintopseudo.csv' in the file output folder (new logs)
# - fills an sqlite
# ! Try running this command twice, and compare the outputs (when no sqlite exists already, or you have removed it). 
# Values that were hashed previously are read from the sqlite, values not seen before are hashed for the 
# first time and stored in the sqlite.
.pdb$process_files()

# Make sure to close the connection
.pdb$close()


#----- Inspect outputs -----

pdb <- shintodb::databaseClass$new(sqlite = "test/sqlite/shinto_pseudomaker.sqlite")
pdb$list_tables()

pdb$read_table("datadienst", lazy = TRUE) %>% head() %>% collect

