use strict;
use warnings;

use ZeroMQ qw/:all/;
use constant {
  REDIS_ZMQ_TYPE_STRING => 0,
  REDIS_ZMQ_TYPE_HASH   => 1,
};

my $cxt = ZeroMQ::Context->new;
my $sock = $cxt->socket('SUB');
$sock->connect("tcp://*:9998");
$sock->setsockopt(ZMQ_SUBSCRIBE, '');

$| = 1;

my $i = 0;
while(1) {
  my @frames = ($sock->recv->data);
  push @frames, $sock->recv->data while $sock->getsockopt(ZMQ_RCVMORE);
  warn("Need 3 message frames, got " . scalar(@frames)), next
    if @frames != 3;
  my ($hdr, $raw_key, $raw_value) = @frames;

  my ($dbnum, $type) = unpack('vv', $hdr);
  my ($key_str) = unpack("V/A", $raw_key);

  my $value;
  if ($type == REDIS_ZMQ_TYPE_STRING) {
    ($value) = unpack("V/A", $raw_value);
  }
  elsif ($type == REDIS_ZMQ_TYPE_HASH) {
    my %h = unpack("V/(V/A)", $raw_value);
    $value = \%h;
  }
  else {
    warn "  GOT UNKNOWN DATA TYPE!";
  }
}

