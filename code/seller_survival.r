library(survival);
data<-read.csv("seller_churn.dat");
msurv <- with(data, Surv(survtime, status == 1));
mfit <- survfit(Surv(survtime, status) ~1, data = data);
summary(mfit);
out<-capture.output(summary(mfit));
cat(out,file="seller_churn_survival.dat",sep="\n",append=FALSE, labels=NULL);
