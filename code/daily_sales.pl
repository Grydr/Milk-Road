#!/usr/bin/perl -w
#
# daily sales with a 29 day sliding windows. 
#
# Used in the paper. Note that this script has NOT been tested with the 
# databases made available publicly, and is absolutely not guaranteed to work. 
# Use at your own risk. 
# 
use Date::Parse;
use Digest::MD5 qw(md5 md5_hex md5_base64);
use DBI;
use DBD::mysql;
use POSIX;

$host = "127.0.0.1";            # CHANGE ME
$database_root = "";            # CHANGE ME
$maindb = "master";             # CHANGE ME
$port = 3306;                   # CHANGE ME
$price_table = "price";
$feedback_table = "feedback";
$user = "root";                 # CHANGE ME
$pw = "";                       # CHANGE ME

# the following table maps a time to the value of the 
# BTC in USD averaged over the past 29 days
#
%conv_table = ( 
        1330819200 => 4.99,
        1330905600 => 4.96,
        1330992000 => 4.94,
        1331078400 => 4.92,
        1331164800 => 4.89,
        1331251200 => 4.87,
        1331337600 => 4.84,
        1331424000 => 4.80,
        1331510400 => 4.78,
        1331596800 => 4.76,
        1331683200 => 4.76,
        1331769600 => 4.78,
        1331856000 => 4.81,
        1331942400 => 4.85,
        1332028800 => 4.87,
        1332115200 => 4.90,
        1332201600 => 4.91,
        1332288000 => 4.92,
        1332374400 => 4.94,
        1332460800 => 4.95,
        1332547200 => 4.94,
        1332633600 => 4.93,
        1332720000 => 4.92,
        1332806400 => 4.91,
        1332892800 => 4.90,
        1332979200 => 4.90,
        1333065600 => 4.90,
        1333152000 => 4.90,
        1333238400 => 4.90,
        1333324800 => 4.91,
        1333411200 => 4.91,
        1333497600 => 4.91,
        1333584000 => 4.91,
        1333670400 => 4.91,
        1333756800 => 4.91,
        1333843200 => 4.90,
        1333929600 => 4.90,
        1334016000 => 4.90,
        1334102400 => 4.90,
        1334188800 => 4.89,
        1334275200 => 4.87,
        1334361600 => 4.86,
        1334448000 => 4.84,
        1334534400 => 4.83,
        1334620800 => 4.82,
        1334707200 => 4.83,
        1334793600 => 4.84,
        1334880000 => 4.85,
        1334966400 => 4.87,
        1335052800 => 4.89,
        1335139200 => 4.90,
        1335225600 => 4.92,
        1335312000 => 4.94,
        1335398400 => 4.95,
        1335484800 => 4.96,
        1335571200 => 4.97,
        1335657600 => 4.97,
        1335744000 => 4.97,
        1335830400 => 4.98,
        1335916800 => 4.98,
        1336003200 => 4.99,
        1336089600 => 5.00,
        1336176000 => 5.00,
        1336262400 => 5.01,
        1336348800 => 5.01,
        1336435200 => 5.02,
        1336521600 => 5.03,
        1336608000 => 5.04,
        1336694400 => 5.04,
        1336780800 => 5.04,
        1336867200 => 5.04,
        1336953600 => 5.04,
        1337040000 => 5.05,
        1337126400 => 5.05,
        1337212800 => 5.06,
        1337299200 => 5.06,
        1337385600 => 5.06,
        1337472000 => 5.05,
        1337558400 => 5.05,
        1337644800 => 5.04,
        1337731200 => 5.04,
        1337817600 => 5.04,
        1337904000 => 5.04,
        1337990400 => 5.05,
        1338076800 => 5.05,
        1338163200 => 5.05,
        1338249600 => 5.06,
        1338336000 => 5.07,
        1338422400 => 5.07,
        1338508800 => 5.08,
        1338595200 => 5.08,
        1338681600 => 5.09,
        1338768000 => 5.09,
        1338854400 => 5.10,
        1338940800 => 5.12,
        1339027200 => 5.13,
        1339113600 => 5.15,
        1339200000 => 5.17,
        1339286400 => 5.19,
        1339372800 => 5.21,
        1339459200 => 5.23,
        1339545600 => 5.26,
        1339632000 => 5.29,
        1339718400 => 5.32,
        1339804800 => 5.37,
        1339891200 => 5.41,
        1339977600 => 5.45,
        1340064000 => 5.49,
        1340150400 => 5.54,
        1340236800 => 5.60,
        1340323200 => 5.64,
        1340409600 => 5.69,
        1340496000 => 5.73,
        1340582400 => 5.77,
        1340668800 => 5.82,
        1340755200 => 5.86,
        1340841600 => 5.91,
        1340928000 => 5.96,
        1341014400 => 6.01,
        1341100800 => 6.06,
        1341187200 => 6.11,
        1341273600 => 6.15,
        1341360000 => 6.19,
        1341446400 => 6.24,
        1341532800 => 6.28,
        1341619200 => 6.32,
        1341705600 => 6.36,
        1341792000 => 6.40,
        1341878400 => 6.45,
        1341964800 => 6.51,
        1342051200 => 6.56,
        1342137600 => 6.62,
        1342224000 => 6.68,
        1342310400 => 6.73,
        1342396800 => 6.78,
        1342483200 => 6.85,
        1342569600 => 6.94,
        1342656000 => 7.03,
        1342742400 => 7.08,
        1342828800 => 7.16,
        1342915200 => 7.22,
        1343001600 => 7.29,
        1343088000 => 7.36,
        1343174400 => 7.44,
        1343260800 => 7.52,
        1343347200 => 7.60,
        1343433600 => 7.67
);

@snapshot_list=qw(1330820000 1330900000 1330990000 1331760000 1331850000 1331940000 1332020000 1332100000 1332190000 1332300000 1332370000 1332460000 1332540000 1332630000 1332710000 1332800000 1332880000 1332970000 1333050000 1333150000 1333230000 1333320000 1333410000 1333490000 1333570000 1333660000 1333750000 1333920000 1334620000 1334780000 1334870000 1334950000 1335050000 1335130000 1335300000 1335650000 1335740000 1335820000 1335990000 1336250000 1336340000 1336430000 1336510000 1336600000 1336690000 1336770000 1336860000 1336940000 1337030000 1337120000 1337210000 1337290000 1337390000 1337460000 1337550000 1337630000 1337730000 1337810000 1337890000 1337980000 1338500000 1338590000 1338670000 1338760000 1339080000 1339190000 1339280000 1339360000 1339450000 1339530000 1339630000 1339710000 1339790000 1339880000 1339970000 1340060000 1340140000 1340230000 1340310000 1340400000 1340490000 1340570000 1340660000 1340750000 1340850000 1340920000 1341000000 1341090000 1341180000 1341270000 1341350000 1341440000 1341520000 1341610000 1341700000 1341780000 1341870000 1341950000 1342040000 1342650000 1342730000 1342820000 1342910000 1343080000); 
@raw=qw(1330815600 1330902000 1330988400 1331766000 1331852400 1331938800 1332025200 1332111600 1332198000 1332284400 1332370800 1332457200 1332543600 1332630000 1332716400 1332802800 1332889200 1332975600 1333062000 1333148400 1333234800 1333321200 1333407600 1333494000 1333580400 1333666800 1333753200 1333926000 1334617200 1334790000 1334876400 1334962800 1335049200 1335135600 1335308400 1335654000 1335740400 1335826800 1335999600 1336258800 1336345200 1336431600 1336518000 1336604400 1336690800 1336777200 1336863600 1336950000 1337036400 1337122800 1337209200 1337295600 1337382000 1337468400 1337554800 1337641200 1337727600 1337814000 1337900400 1337986800 1338505200 1338591600 1338678000 1338764400 1339023600 1339196400 1339282800 1339369200 1339455600 1339542000 1339628400 1339714800 1339801200 1339887600 1339974000 1340060400 1340146800 1340233200 1340319600 1340406000 1340492400 1340578800 1340665200 1340751600 1340838000 1340924400 1341010800 1341097200 1341183600 1341270000 1341356400 1341442800 1341529200 1341615600 1341702000 1341788400 1341874800 1341961200 1342047600 1342652400 1342738800 1342825200 1342911600 1343084400); 

&main(); 

sub main {
        my $dbh;
        my $dbh2;
        my $sth; 
        my $sth2; 

        foreach $snapshot (@snapshot_list) { 
                $database=$database_root.$snapshot;
                $dbh = DBI->connect("DBI:mysql:database=$database;host=$host;port=$port", $user, $pw) or die "Cannot connect to MySQL server\n";
                $dbh2 = DBI->connect("DBI:mysql:database=$maindb;host=$host;port=$port", $user, $pw) or die "Cannot connect to MySQL server\n";
                $ts = shift(@raw); 
                my (%price) = (); 
                $count = 0;
                $count_all_items = 0;
                $avg_price = 0;
                $total_price = 0;
                $actual_cut = 0;
                $actual_cut_usd = 0;
                $cut_linear = 0;
                $cut_linear_usd = 0;

                $sth = $dbh->prepare("SELECT feedback.item_id, COUNT(feedback.item_id) from $feedback_table where feedback.feedback_time >= ? && feedback.feedback_time < ? GROUP BY (feedback.item_id)") 
                        or die "Couldn't prepare statement: " . $dbh->errstr;
                $sth->execute($ts-86400*29, $ts) 
                        or die "Couldn't execute statement: " . $sth->errstr;

                while (@data = $sth->fetchrow_array()) {
                       $id = $data[0];
                       $sold = $data[1];
                       $count += $sold; 

                       # calculate the average price found for this item at the time the feedback was deposited

                       undef($avg_price_item); 

                       unless ($price{$id}) { 
                               $sth2 = $dbh2->prepare("SELECT AVG(price.price) from $price_table where price.item_id = ? && price.time >= ? && price.time < ?")
                                       or die "Couldn't prepare statement: " . $dbh->errstr;
                               $sth2->execute($id,$ts-86400*29,$ts)
                                       or die "Couldn't execute statement: " . $sth2->errstr;
                               $avg_price_item = $sth2->fetchrow_array();
                               $sth2->finish(); 

                               if ($avg_price_item) {
                                       $price{$id} = $avg_price_item;
                               } else {
                                       print STDERR ("Warning: No price for item $id at $ts..."); 
                                       # attempt to find an approximation from the current snapshot
                                       $sth2 = $dbh->prepare("SELECT AVG(price.price) from $price_table where price.item_id = ?") 
                                               or die "Couldn't execute statement: ".$sth2->errstr; 
                                       $sth2->execute($id)
                                               or die "Couldn't execute statement: " . $sth2->errstr;
                                       $avg_price_item = $sth2->fetchrow_array();
                                       $sth2->finish(); 
                                       # the 1000 BTC limit below is somewhat self-explanatory: we're looking at an item that only has one price posted over the past month, and it is really expensive; most likely the seller just increased the price a lot to keep stats, but doesn't sell anymore
                                       #
                                       
                                       if ($avg_price_item && $avg_price_item < 1000) {
                                               $price{$id} = $avg_price_item;
                                               print STDERR " Fixed!\n"; 
                                       } else {
                                               $avg_price_item = 0; 
                                               print STDERR ("\n"); 
                                       }
                               }
                       } else {
                               $avg_price_item = $price{$id};
                       }
                       if ($avg_price_item && &btc2usd($avg_price_item,$ts) > 6000) {
                               print STDERR "Warning: item=$id, ts=$ts, very high price p=$avg_price_item BTC -- may need to investigate\n"; 
                       }
                       if ($avg_price_item) { 
                               $cut_linear += $sold*$avg_price_item*0.0623;
                               $cut_linear_usd += $sold*&btc2usd($avg_price_item*0.0623,$ts);
                               
                               # look up btc/usd conversion for that timestamp;
                               # convert and check 
                               
                               my $usd_price = &btc2usd($avg_price_item,$ts);
                               if ($usd_price <= 50) {
                                       $increment = 0.1*$usd_price; 
                                       $actual_cut += $sold*&usd2btc($increment,$ts); 
                                       $actual_cut_usd += $sold*$increment; 
                               } elsif ($usd_price > 50 && $usd_price <= 150) {
                                       $increment = 50*0.1+0.085*($usd_price-50); 
                                       $actual_cut += $sold*&usd2btc($increment,$ts); 
                                       $actual_cut_usd += $sold*$increment; 

                               } elsif ($usd_price > 150 && $usd_price <= 300) {
                                       $increment = 50*0.1+0.085*100+0.06*($usd_price-150); 
                                       $actual_cut += $sold*&usd2btc($increment,$ts); 
                                       $actual_cut_usd += $sold*$increment; 
                               } elsif ($usd_price > 300 && $usd_price <= 500) {
                                       $increment = 50*0.1+100*0.085+150*0.06+0.03*($usd_price-300); 
                                       $actual_cut += $sold*&usd2btc($increment,$ts); 
                                       $actual_cut_usd += $sold*$increment; 
                               } elsif ($usd_price > 500 && $usd_price <= 1000) {
                                       $increment = 50*0.1+100*0.085+150*0.06+0.03*200+0.02*($usd_price-500); 
                                       $actual_cut += $sold*&usd2btc($increment,$ts); 
                                       $actual_cut_usd += $sold*$increment; 
                               } elsif ($usd_price > 1000) {
                                       $increment = 50*0.1+100*0.085+150*0.06+0.03*200+0.02*500+0.015*($usd_price-1000); 
                                       $actual_cut += $sold*&usd2btc($increment,$ts); 
                                       $actual_cut_usd += $sold*$increment; 
                               }
                               $total_price += $sold*$avg_price_item;
                               $count_all_items += $sold;
                       }
                } 
                if ($count_all_items) {
                       $avg_price = $total_price/(1.0*$count_all_items);  
                }
                printf STDOUT "%d\t%d\t%d\t%.5f\t%d\t%.5f\t%.5f\t%.5f\t%.5f\n", $ts, $count, $count_all_items, $avg_price, $total_price, $cut_linear, $actual_cut, $cut_linear_usd, $actual_cut_usd; 
                $dbh->disconnect();
                $dbh2->disconnect();
        }
        return(-1);
}
sub btc2usd {
        $value = $_[0];
        $time = $_[1]; 
        #
        # there is a slight misalignment of the bitcoincharts data
        # and what we need: one hour (over one month)
        # the difference is considered negligible
        #
        return ($value*$conv_table{$time+3600});

}
sub usd2btc {
        $value = $_[0];
        $time = $_[1]; 
        return ($value/$conv_table{$time+3600});
}

