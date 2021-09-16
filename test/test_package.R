
library(shintopseudo)
.pdb <- pseudoDB$new("test/config_ssd.yml", secret = "banaan")
.pdb$process_files()
.pdb$close()

