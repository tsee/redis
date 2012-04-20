use strict;
use warnings;

use ZeroMQ qw/:all/;
my $cxt = ZeroMQ::Context->new;
my $sock = $cxt->socket('SUB');
$sock->connect("tcp://*:9998");

$| = 1;

$sock->setsockopt(ZMQ_SUBSCRIBE, '');

my $i = 0;
while(1) {
  my $data = $sock->recv->data;
  next if not defined $data or length($data) == 0;
  my ($type, $key, $val) = unpack("nL/aL/a", $data);
  print "TYPE: " . $type . "\n";
  ++$i;
  print "$i key='$key' data='$val'\n"; #if not $i % 1000;
}

