
# 4) paste the entire R script, save, exit

# plot_percentiles.R
# Regenerates percentile curves with a light ggplot theme
# -------------------------------------------------------------------

library(tidyverse)
library(readr)

# ------------ CONFIG -------------------------------------------------
PCT_CSV   <- "trend_tables/mic_percentiles.csv"   # source table
OUT_DIR   <- "plots"                              # where PNGs go
BR_YEAR   <- 2004                                 # x-axis min

# Colour palette (edit anytime)
COL_P90 <- "#D62728"   # red
COL_P50 <- "#1F77B4"   # blue
COL_P10 <- "#555555"   # dark grey
# ---------------------------------------------------------------------

dir.create(OUT_DIR, showWarnings = FALSE)

pct <- read_csv(PCT_CSV, show_col_types = FALSE)

# build full organism-drug list you want; here we filter by tigecycline
targets <- pct %>%
  filter(str_to_lower(drug) == "tigecycline") %>%
  distinct(organism, drug)

for (i in seq_len(nrow(targets))) {
  org <- targets$organism[i]
  dr  <- targets$drug[i]

  combo <- pct %>%
    filter(organism == org, drug == dr) %>%
    arrange(year)

  p <- ggplot(combo, aes(x = year)) +
    geom_line(aes(y = p90,    colour = "90th pct."),  linewidth = 1) +
    geom_line(aes(y = median, colour = "50th pct."),  linewidth = 1, linetype = "dashed") +
    geom_line(aes(y = p10,    colour = "10th pct."),  linewidth = 1, linetype = "dotdash") +
    scale_colour_manual(
      "", values = c("90th pct." = COL_P90,
                     "50th pct." = COL_P50,
                     "10th pct." = COL_P10)
    ) +
    scale_y_log10() +
    coord_cartesian(xlim = c(BR_YEAR, NA)) +
    labs(title = paste(org, "–", dr),
         x = "Year",
         y = "MIC (µg/mL)") +
    theme_light(base_size = 12) +
    theme(
      plot.title      = element_text(face = "bold"),
      legend.position = "bottom"
    )

  fname <- file.path(
    OUT_DIR,
    paste0(gsub("[^A-Za-z0-9]+", "_", org), "_",
           gsub("[^A-Za-z0-9]+", "_", dr), ".png")
  )
  ggsave(fname, p, width = 7, height = 4, dpi = 150)
  message("Wrote ", fname)
}

