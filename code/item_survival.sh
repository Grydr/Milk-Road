#!/bin/bash
tmp=/tmp/$$
echo "survtime,status";
cat ../data/item-churn-censored.txt | awk '{printf("%d, 0\n", $2+0.5)}' > $tmp
cat ../data/item-churn-noncensored.txt | awk '{printf("%d, 1\n", $2+0.5)}' >> $tmp
sort -n $tmp 
rm $tmp
