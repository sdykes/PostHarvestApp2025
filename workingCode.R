library(tidyverse)

df <- expand.grid(x = c("squat", "normal", "long", "penguin"), 
                  y = c("US", "US sample", "53mm", "58mm", "63 small", "63 medium", "63 large", "67mm", "72mm", "OS sample", "OS"))

df$US <- c(rep(1,4),rep(0,40)) 
df$`US sample` <- c(rep(0,4), rep(1,4), rep(0,36))
df$`53/2` <- c(rep(0,8), rep(1,4), rep(0,32))
df$`53/5` <- c(rep(0,8), rep(1,4), rep(0,32))
df$`58mm` <- c(rep(0,12), rep(1,4), rep(0,28))
df$`family pack` <- c(rep(0,4), rep(1,14), rep(0,2), 1, rep(0,23))
df$`63/5` <- c(rep(0,16), rep(1,6), rep(0,2), rep(1,2), rep(0,18))
df$`63/5T` <- c(rep(0,18), rep(1,2), 0, rep(1,7), rep(0,16))
df$`63/3` <- c(rep(0,20), rep(c(0,0,1,1),2), rep(0,16))
df$`63/4` <- c(rep(0,24), rep(1,4), rep(0,16))
df$`67/5` <- c(rep(0,28), rep(1,4), rep(0,12))
df$`67/4` <- c(rep(0,28), rep(1,4), rep(0,12))
df$`72/4` <- c(rep(0,32), rep(1,4), rep(0,8))
df$`OS sample` <- c(rep(0,36), rep(1,4), rep(0,4))
df$OS <- c(rep(0,40), rep(1,4)) 

df %>%
  pivot_longer(cols = c(US:OS), names_to = "SKU", values_to = "logic") |>
  mutate(SKU = factor(SKU, 
                      levels = c("US", "US sample", "53/2", "53/5", "58mm", "family pack", "63/5", "63/5T", "63/3", "63/4", 
                                 "67/5", "67/4", "72/4", "OS sample", "OS")),
         logic2 = case_when(SKU %in% c("US","US sample","58mm","family pack","72/4", "OS sample", "OS") & logic == 1 ~ "active bin",
                            SKU %in% c("53/2", "53/5", "67/4", "67/5") & logic == 1 ~ "overlapped",
                            SKU == "63/4" & logic == 1 ~ "overlapped",
                            SKU == "63/3" & logic == 1 ~ "overlapped",
                            SKU == "63/5" & x %in% c("long","penguin") & y == "63 small" & logic == 1 ~ "overlapped",
                            SKU == "63/5" & x == "normal" & y == "63 medium" ~ "overlapped",
                            SKU == "63/5" & x %in% c("squat", "normal") & y == "63 large" ~ "overlapped",
                            SKU %in% c("63/5", "63/5T") & logic == 1 ~ "active bin",
                            TRUE ~ "independent")) |>
  ggplot(aes(x, y)) +
  geom_tile(aes(fill = factor(logic2)), colour="grey50") +
  facet_wrap(~SKU, ncol = 3) +
  labs(x = "elongation bins",
       y = "equatorial bins") +
  scale_fill_manual(values=c("dark blue", "white","dark blue")) +
  theme(legend.position = "none",
        axis.text.y = element_text(size = 7))

df2 <- df |>
  mutate(index = row_number())

df2 |>
  ggplot(aes(x=x, y=y)) +
  geom_tile(fill="grey90", colour = "black", linewidth=1) +
  geom_text(aes(label = index), size = 4.0, colour = "black") +
  labs(x = "Elongation cuts",
       y = "Equatorial diameter cuts")


diameterCuts <- tibble(`Bin name` = c("undersize", "small samples", "53/5", "58/5", "63small", "63medium", "63big", "67/4", "72/4", "large sample", "oversize"),
                       `min. / mm` = c(0, 44.0, 47.0, 51.2, 54.4, 56.1, 59.0, 61.7, 66.2, 71.8, 79.0),
                       `max. / mm` = c(44.0, 47.0, 51.2, 54.4, 56.1, 59.0, 61.7, 66.2, 71.8, 79.0, 100)) 

elongationCuts <- tibble(`Bin name` = c("squat", "normal", "long", "penguin"),
                         min = c(0.0, 0.865,0.942, 0.960),
                         max = c(0.865,0.942, 0.960, 1.2))

diameterCuts |>
  flextable::flextable() |>
  flextable::bold(bold=TRUE, part = "header") |>
  flextable::autofit()

elongationCuts |>
  flextable::flextable() |>
  flextable::bold(bold=TRUE, part = "header") |>
  flextable::autofit()

tibble(SKUNames = c("53/5","58/5","Family pack small","Family pack large","63/5N", "63/5T", "63/3", "63/4", "67/5","72/4"),
       SKUSets = c("$X_{53} \\in \\{9:12\\}$",
                   "$X_{58} \\in \\{13:16\\}$",
                   "$X_{FPS} \\in \\{9:20\\}$",
                   "$X_{FPL} \\in \\{9:21,25\\}$",
                   "$X_{63N} \\in \\{17:22,25,26\\}$",
                   "$X_{63T} \\in \\{19,20,22:28\\}$",
                   "$X_{633} \\in \\{23,24,27,28\\}$",
                   "$X_{634} \\in \\{25:28\\}$",
                   "$X_{675} \\in \\{29:32\\}$",
                   "$X_{724} \\in \\{33:36\\}$")) |>
  kableExtra::kbl(col.names = c("SKU names", "SKU sets by index"),
                  escape = F,
                  booktabs = T,
                  linesep = "") |>
  kableExtra::kable_styling(full_width=F)











