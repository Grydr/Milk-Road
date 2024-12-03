-- Unless otherwise noted, all these plots use the master database. 
-- All code here is SQL. Some additional scripts are used to parse these files.
-- Contact the author if you want to have access to these additional scripts.
--
-- Figure 3/Table 1:
--
select category, count(item_id) from item group by category into outfile "cat.csv";
--
-- (subsequently post-process to plot CDF; note some categories have
-- duplicates as shown in the categories.csv file, those were recombined 
-- manually)
--
-- Figure 4:
--
select item_id, (last_seen-first_seen)/(3600*24), last_seen from item where last_seen<1343080000 order by (last_seen-first_seen) into outfile "item-churn-noncensored.txt";
-- 
select item_id, (last_seen-first_seen)/(3600*24), last_seen from item where last_seen>=1343080000 order by (last_seen-first_seen) into outfile "item-churn-censored.txt";
--
-- then run item_survival.sh
-- and finally get the R output by item_survival.r
--
-- Figure 5:
-- see sellers_evolution.pl 
-- use seller_linear.r to get the linear regression from R
-- 
-- Figure 6:
select seller, item_id, first_seen, last_seen from item order by seller,first_seen into outfile "seller_churn_raw.txt"; 
-- then run seller_survival.pl
-- and finally get the R output by seller_survival.r
-- Table 2: 
select ships_from, count(ships_from), count(ships_from)*100/24422. from item group by ships_from order by count(ships_from) desc into outfile "ships_from.csv";
select ships_to, count(ships_to) from item group by ships_to order by count(ships_to) desc into outfile "ships_to.csv";
-- then manual post-processing to combine certain regions
--
-- Figure 7:
select seller,count(item_id) from item group by(seller) order by count(item_id) desc into outfile "seller_item_raw.csv"; 
-- then postprocess to get the CDFs
--
-- Figure 8:
select item.seller, count(feedback.item_id) from item,feedback where feedback.item_id=item.item_id group by (item.seller) order by count(feedback.item_id) desc into outfile "seller_vol_raw.csv"; 
-- then postprocess to get the CDFs
-- 
-- Table 3:
select feedback_rating, count(feedback.item_id) from feedback group by (feedback_rating) order by feedback_rating desc;
-- 
-- Figure 10: bitcoins; data is from Bitcoin charts
-- 
-- Figure 11:
-- get the items in the basket:
select item_id, count(item_id) from feedback group by item_id order by count(item_id);
-- get their prices over time: 
select time,price from price where item_id=XXX into outfile "priceXXX.csv";
-- (replace XXX by the item_ids you found in the first line)
--
-- Figures 12 and 13: 
-- see daily_sales.pl 
