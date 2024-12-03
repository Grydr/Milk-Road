#!/opt/local/bin/perl
#
use Math::Round qw/round/;

open READER, "seller_churn_raw.txt";
$old_seller=`head -n 1 seller_churn_raw.txt | awk '{print \$1;}'`;
$old_starttime=`head -n 1 seller_churn_raw.txt | awk '{print \$3;}'`;
$THRESH = 1343080000;
chomp($old_seller);
chomp($old_starttime);
print "survtime,status\n";
while (<READER>) {
        my @values=split(' ', $_);
        $seller = $values[0];
        $starttime = $values[2];
        $endtime = $values[3];
        if ($seller ne $old_seller) {
                $duration = round(($old_endtime-$old_starttime)/(3600.*24));
                $censored = ($old_endtime >= $THRESH)?0:1;
                print "$duration, $censored\n"; 
                $old_seller = $seller;
                $old_starttime = $starttime;
                $old_endtime = $endtime;
        }
        if ($starttime < $old_starttime) {
                $old_starttime = $starttime;
        }
        if ($endtime > $old_endtime) {
                $old_endtime = $endtime;
        }
}
close(READER);
$duration = round(($old_endtime-$old_starttime)/(3600.*24));
$censored = ($old_endtime >= $THRESH)?0:1;
print "$duration, $censored\n"; 
