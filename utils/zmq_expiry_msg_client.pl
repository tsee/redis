use strict;
use warnings;

use ZeroMQ qw/:all/;
my $cxt = ZeroMQ::Context->new;
my $sock = $cxt->socket('SUB');
$sock->connect("tcp://*:9998");

$| = 1;

$sock->setsockopt(ZMQ_SUBSCRIBE, '');

use constant {
  REDIS_ZMQ_TYPE_STRING => 0,
  REDIS_ZMQ_TYPE_HASH   => 1,
};

my $i = 0;
while(1) {
  my $hdr = $sock->recv->data;
  if (not $sock->getsockopt(ZMQ_RCVMORE)) {
    warn "GOT ONLY ONE MESSAGE FRAME!!!";
    next;
  }
  my $raw_key = $sock->recv->data;
  if (not $sock->getsockopt(ZMQ_RCVMORE)) {
    warn "GOT ONLY TWO MESSAGE FRAMES!!!";
    next;
  }
  my $raw_data = $sock->recv->data;

  next if not defined $raw_data or length($raw_data) == 0;
  my ($dbnum, $type) = unpack('vv', $hdr);
  my ($k) = unpack("V/A", $raw_key);
  print "dbnum='$dbnum' type='$type' key='$k'\n";
  if ($type == REDIS_ZMQ_TYPE_STRING) {
    my ($v) = unpack("V/A", $raw_data);
    print "  STRING. value='$v'\n";
  }
  elsif ($type == REDIS_ZMQ_TYPE_HASH) {
    my %h = unpack("V/(V/A)", $raw_data);
    print "  HASH.\n";
    print "    $_ => $h{$_}\n" for keys %h;
  }
  else {
    warn "  GOT UNKNOWN DATA TYPE!";
  }

  #my ($type, $key, $val) = unpack("nL/aL/a", $data);
  #print "TYPE: " . $type . "\n";
  #++$i;
  #print "$i key='$key' data='$val'\n"; #if not $i % 1000;
}

