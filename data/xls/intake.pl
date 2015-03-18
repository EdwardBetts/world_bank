#! /usr/bin/perl

use strict;
use warnings;
use Text::CSV;
use Data::Dumper qw(Dumper);

open(REFFILE,$ARGV[0]) or die;
my %cvalues;
my $naval = -9.9;
while (<REFFILE>) {
    chomp;
    next if (/^\#/);
    next if (/^$/);
    s/^\s+//;
    s/\s+$//;
    my $csv = Text::CSV->new({ sep_char => ',' });
    open(my $infile, '<', $_) or die;
    open(my $outfile, '>', $ARGV[1]) or die;
    my $metric = $_;
    $metric =~ s/\..*//;
    $metric =~ s/^.*\///;
    while (<$infile>) {
	chomp;
	next if (/^\./);    # skip descriptor lines
	next if (/^$/);     # skip blank lines
	s/\s+//g;           # remove all whitespace
	s/[#%&\$*+()]//g; # get rid of special characters
	if ($csv->parse($_)) {
	    my @fields = $csv->fields();
	    @fields = map { lc } @fields;
	    @fields = grep { $_ ne '' } @fields;
	    for (@fields) {
		s/^-$/$naval/g;         # replace '-' w/ N/A value
		s/\:.*//g;            # remove text
		s/[\,#\-%&\$*+()]//g; # remove remaining special characters
		s/n\/a/$naval/gi;       # get rid of N/A's
	    }
	    $cvalues{$fields[1]}{$metric} = pop(@fields);
	    print $outfile $_."," for @fields;
	    print $outfile "\n";
	} else {
	    warn "Line could not be parsed: $_\n";
	}
    }
    close $outfile;
}
close REFFILE;

open(OUTFILE, ">out.dat") or die;
#print scalar(keys %cvalues);
foreach my $country (sort keys %cvalues) {
    print OUTFILE "$country,";
    foreach my $measure (keys %{ $cvalues{$country} }) {
        print OUTFILE "$cvalues{$country}{$measure},";
    }
    print OUTFILE "\n";
}
close OUTFILE;
