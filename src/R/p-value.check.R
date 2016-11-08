# загрузка данных
cpm_data <- read.csv("~/Downloads/query_result (46).csv")

# функция для расчёта p-value
pvalue_tdist <- function(m, s, n, threshold = 10){
  # m - mean
  # s - sd
  # n - n
  
  xbar <- m * (1 + threshold / 100)
  t <- (xbar - m) / (s / sqrt(n))
  result <- 2*pt(-abs(t), df=n-1)
  return(result)
}

# считаем p-value
cpm_data$pvalue <- pvalue_tdist(m = cpm_data$cpm_avg, s = cpm_data$sd, n = cpm_data$n)
cpm_data$pvalue <- ifelse(cpm_data$sd == 0, 0, cpm_data$pvalue)

cpm_data_good <- cpm_data[cpm_data$pvalue < 0.05, ]

write.cs
