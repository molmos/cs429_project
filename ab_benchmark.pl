#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Long;

## Global Vars ##
my ($host,$port,$shards,$query_file,$concurrency,$repeat,$debug,$help);


####################
### Main Routine ###
####################

# Get command-line options
GetOptions(
	"host|h=s",        \$host,
	"port|p=i",        \$port,
	"shards|s=i",      \$shards,
	"query_file|q=s",  \$query_file,
	"concurrency|c=i", \$concurrency,
	"repeat|r=i",      \$repeat,
	"debug|d",         \$debug,
	"help|?",          \$help,
) or usage();

# Check command-line options
usage() if defined($help);
print "Host not given. See -host below:\n"               and usage() unless defined($host);
print "Port not given. See -port below:\n"               and usage() unless defined($port);
print "Shards not given. See -shards below:\n"           and usage() unless defined($shards);
print "Query File not given. See -query_file below:\n"   and usage() unless defined($query_file);
print "Concurrency not given. See -concurrency below:\n" and usage() unless defined($concurrency);
print "Repeat not given. See -repeat below:\n"           and usage() unless defined($repeat);


# Debug command-line options
debug("Host:        $host");
debug("Port:        $port");
debug("Shards:      $shards");
debug("Query File:  $query_file");
debug("Repeat:      $repeat");
debug("Concurrency: $concurrency");


# Open the query file and create runs for each query
my $time = time;
open(QRY, "$query_file") or die "Cannot open $query_file: $!\n";
while (<QRY>) {
	my $q = $_;
	chomp($q);
	debug("Found query: $q\n");

	run_query($q);
}
close(QRY) or die "Cannot close $query_file: $!\n";

exit;

#########################
# run_query sub routine #
#########################
sub run_query {
	my ($query) = shift;

#### Use the following for urls:
	my $url;

	if ($shards = 1) {
		$url = "http://$host:$port/solr/search?q=$query&df=text";
	} else {
		$url = "http://$host:$port/solr/dist-query?q=$query&df=text";
	}

	#my $url     = "http://$host:$port/solr/select?shards=localhost:9910/solr,localhost:9911/solr,localhost:9912/solr,localhost:9913/solr&indent=true&q=$query";

	my $cmd = "ab -n $repeat -c $concurrency '$url' >> tmp_$time.txt";
	debug("Running CMD: $cmd");

	my $response = `$cmd`;
}

#####################
# usage sub routine #
#####################
sub usage {
	my $progname = `basename $0`;
	chomp $progname;

	print "
	usage: $progname
	Script for performing benchmarking on Solr using ApacheBench (ab)

	-host|h        The host's url
	-port|p        The host's port
	-shards|s      The number of shards being used at the host
	-query_file|q  The file which has the list of queries to perform
	-concurrency|c The number of requests to send simultaneously
	-repeat|r      The number of times to perform each query
	-debug|d       Debug mode
	-help|?        This message\n\n";

	exit 1;
}

#####################
# debug sub routine #
#####################
sub debug {
	my $debug_msg = shift;
	chomp $debug_msg;

	print "DEBUG: $debug_msg\n" if defined($debug);
}

