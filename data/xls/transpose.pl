#! /usr/bin/perl

use strict;
use warnings;
use Text::CSV;
use Array::Transpose;

my @transposed;
my $csv = Text::CSV->new({ sep_char => ',' });
open(my $infile, '<', $ARGV[0]) or die;
while (my $row = $csv->getline($infile)) {
    push @transposed, $row;
}
@transposed = transpose(\@transposed);
my $tout = $ARGV[0]."_transposed";
open(my $fh, ">:encoding(utf8)", $tout) or die;
for (@transposed) {
    $csv->print($fh, $_);
    print $fh "\n";
}
close $fh;
