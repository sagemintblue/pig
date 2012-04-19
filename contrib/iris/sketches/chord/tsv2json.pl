#!/usr/bin/perl

use strict;
use warnings;

sub json {
    local $_ = shift();
    chomp;
    return "[" . join(",", map {"\"$_\""} grep { length($_) > 0} split(/,/)) . "]";
}

print "[\n";
while (<>) {
    my ($jobid, $maps, $reduces, $maxmaptime, $minmaptime, $avgmaptime, $maxreducetime, $minreducetime, $avgreducetime, $aliases, $features, $outputs) = split(/\t/);
    $aliases = json($aliases);
    $features = json($features);
    $outputs = json($outputs);
    print ",\n" unless ($. == 1);
    my $line = <<"EOT";
{"jobid":"$jobid","maps":$maps,"reduces":$reduces,"maxmaptime":$maxmaptime,"minmaptime":$minmaptime,"avgmaptime":$avgmaptime,"maxreducetime":$maxreducetime,"minreducetime":$minreducetime,"avgreducetime":$avgreducetime,"aliases":$aliases,"features":$features,"outputs":$outputs}
EOT
    chomp($line);
    print $line;
}
print "\n]\n";
