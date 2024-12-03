in_data <- read.csv("seller_counts.csv",head=FALSE);
days=(in_data$V1-in_data$V1[1])/(3600*24)
sellers=in_data$V2
res=lm(sellers ~ days)
summary(res)
plot (days,sellers);
abline(res);
