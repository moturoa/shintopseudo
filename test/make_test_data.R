
library(randNames)
library(dplyr)

data <- randNames::rand_names(100, nationality = "NL")

out1 <- data %>%
  slice(1:60) %>%
  select(voornaam = name.first, achternaam = name.last, geslacht = gender, email_adres = email, telnr = phone, leeftijd = dob.age)

out2 <- data %>%
  slice(40:100) %>%
  select(voornamen = name.first, geslachtsnaam = name.last, geslacht = gender, email = email, telnr = phone, postcode = location.postcode)
         
write.csv2(out1, "test/brp_test_data1.csv", row.names = FALSE)
write.csv2(out2, "test/brp_test_data2.csv", row.names = FALSE)




