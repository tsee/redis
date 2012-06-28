#system(q{perl -le '$y=$_**2, $x .= "SET a$y $y\nEXPIRE a$y 1\n" for 1..50000; print $x' | bin/redis-cli > /dev/null});

$r=rand(), $y=$_**2, $x .= "HSET A${_}Z$r foo a$y\nHSET A${_}Z$r bar b$y\nHSET A${_}Z$r baz c$y\nEXPIRE A${_}Z$r 3\n" for 1..($ARGV[0]||1);
#$r=rand(), $y=$_**2, $x .= "SET test$_$r val$_\nEXPIRE test$_$r 1\nHSET A${_}Z$r foo a$y\nHSET A${_}Z$r bar b$y\nHSET A${_}Z$r baz c$y\nEXPIRE A${_}Z$r 1\n" for 1..($ARGV[0]||1);
#$r=rand(), $y=$_**2, $x .= "SET test$_$r val$_\nEXPIRE test$_$r 1\n" for 1..($ARGV[0]||1);

open my $fh, "| bin/redis-cli > /dev/null" or die $!;
print $fh $x;
close $fh;

