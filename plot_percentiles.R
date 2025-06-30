#!/usr/bin/env Rscript

# plot_percentiles.R
# ------------------
# Reads trend_tables/mic_percentiles.csv and makes a ggplot2 log–scale percentile curve.

library(tidyverse)

# 1) Read the percentiles table
pct <- read_csv("trend_tables/mic_percentiles.csv")

# 2) Choose your combo here:
organism <- "Escherichia coli"
drug     <- "Tigecycline"

combo <- pct %>%
  filter(organism == !!organism, drug == !!drug)

if (nrow(combo) == 0) {
  stop("No data found for ", organism, " + ", drug)
}

# 3) Plot
p <- ggplot(combo, aes(x = year)) +
  geom_line(aes(y = p90,    colour="90th pct."),    size = 1) +
  geom_point(aes(y = p90,   colour="90th pct."),    size = 2) +
  geom_line(aes(y = median, colour="50th pct."),    linetype="dashed", size = 1) +
  geom_point(aes(y = median,colour="50th pct."),    size = 2) +
  geom_line(aes(y = p10,    colour="10th pct."),    linetype="dotdash", size = 1) +
  geom_point(aes(y = p10,   colour="10th pct."),    size = 2) +
  scale_y_log10() +
  scale_colour_manual("", values=c("10th pct."="grey40","50th pct."="blue","90th pct."="red")) +
  labs(
    title = paste(organism, "–", drug, "MIC Percentiles Over Time"),
    x = "Year", y = "MIC (µg/mL)"
  ) +
  theme_minimal() +
  theme(legend.position="bottom", plot.title=element_text(hjust=0.5))

# 4) Save
dir.create("plots", showWarnings = FALSE)
out_file <- file.path("plots", paste0(gsub(" ", "_", organism), "_", gsub(" ", "_", drug), "_percentiles.png"))
ggsave(out_file, plot=p, width=8, height=5, dpi=150)
message("Saved plot to ", out_file)
