use strict;
use warnings;

use ZeroMQ qw/:all/;
my $cxt = ZeroMQ::Context->new;
my $sock = $cxt->socket('SUB');
$sock->bind("tcp://*:9998");

$| = 1;

$sock->setsockopt(ZMQ_SUBSCRIBE, '');

my $i = 0;
while(1) {
  my $data = $sock->recv->data;
  next if not defined $data or length($data) == 0;
  my ($key, $val) = unpack("L/aL/a", $data);
  ++$i;
  print "$i key='$key' data='$val'\n"; #if not $i % 1000;
}

