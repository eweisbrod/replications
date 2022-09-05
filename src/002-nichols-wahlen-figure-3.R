# Setup ------------------------------------------------------------------------

# Load Libraries [i.e., packages]
library(modelsummary)
library(kableExtra)
library(formattable)
library(lubridate)
library(glue)
library(haven)
library(fixest)
library(tictoc) #very optional, mostly as a teaching example
library(tidyverse) # I like to load tidyverse last to avoid package conflicts

library(modelsummary)

#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")


# read in the data from SAS ----------------------------------------------------


figdata <- read_sas(glue("{data_path}/figdata2.sas7bdat"))


# replicate Nichols and Wahlen figure -------------------------------------------


fig <- figdata |>
  #tell R that the deciles are categorical not continuous
  #add some stuff for labels from Ian Gow book
  mutate(dec_ue = factor(dec_ue+1),
         last_day = td_days == max(td_days),
         label = if_else(last_day, as.character(dec_ue), NA_character_)
         ) |> 
  ggplot(aes(x = td_days, 
             y= mean_bhar,
             color = dec_ue,
             linetype = dec_ue,
             group = dec_ue)) + 
  geom_line() + geom_vline(xintercept=0) + 
  scale_y_continuous(name = "Mean Buy-and-Hold Abnormal Return", labels = scales::percent) +
  scale_x_continuous(name = "Days Relative to Earnings Announcement", breaks = seq(-5,5,1)) +
  geom_label(aes(label = label), na.rm = TRUE) +
  theme_bw(base_family = "serif") +
  theme(legend.position = "none")

#Look at it in R  
fig

#For Latex
ggsave(glue("{data_path}/nichols_wahlen_fig3.pdf"), fig, width = 7, height = 6)

#For Word
ggsave(glue("{data_path}/output/nichols_wahlen_fig3.png"), fig, width = 4.2, height = 3.6)

