#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Long;
use JSON;
use Solr;
use threads;
use threads::shared;

## Global Vars ##
my (%solr,%hosts);
my (@files,@threads);
my ($config_file,$files_to_index,$log_dir,$threads,$debug,$help);
my $kill_threads                  : shared = 0;
my $global_amount_of_data_indexed : shared = 0;
my $global_num_frcr_indexed       : shared = 0;

$SIG{'INT'}  = \&Handler;
$SIG{'TERM'} = \&Handler;


####################
### Main Routine ###
####################

# Get command-line options
GetOptions(
	"config|c=s",  \$config_file,
	"files|f=s",   \$files_to_index,
	"log|l=s",     \$log_dir,
	"threads|t=i", \$threads,
	"debug|d",     \$debug,
	"help|h|?",    \$help,
) or usage();

# Check command-line options
usage() if defined($help);
print "Config File Required. See -config for more info.\n\n"            and usage() unless defined($config_file);
print "Files to index are required. See -files for more info.\n\n"      and usage() unless defined($files_to_index);
print "Log directory is required. See -log for more info.\n\n"          and usage() unless defined($log_dir);
print "Amount of threads is required. See -threads for more info.\n\n"  and usage() unless defined($threads) and $threads > 0;
print "Config File does not exist.\n\n"                                 and usage() unless -e $config_file;
print "Directory with files to index does not exist.\n\n"               and usage() unless -d $files_to_index;
print "Specified log directory does not exist.\n\n"                     and usage() unless -d $log_dir;

print "REMINDER: Script should be run as: \n\t~\$ PERL_SIGNALS=unsafe perl ./distributedIndexer3.pl [OPTIONS ... ]\n";


# Debug command-line options
debug("Config File: $config_file");
debug("Directory with Files: $files_to_index");
debug("Log Directory: $log_dir");
debug("Threads: $threads");


# Get the contents of the config file
my $contents = '';
open(CON, $config_file) or die "Cannot open $config_file: $!\n";
while (<CON>) {
	$contents .= $_;
}
close(CON) or die "Cannot close $config_file: $!\n";
die "Error parsing JSON in config file. No hosts found\n" if $contents eq '';


# Parse the json for list of master hosts/ports
my $json         = JSON->new->allow_nonref;
my $decoded_data = $json->decode( $contents );
my $curr_shard   = 0;
my $h            = 1;
my $found_host   = 0;
while (1) {

	if ( exists($decoded_data->[$curr_shard][0][0][4]) and exists($decoded_data->[$curr_shard][0][1]) ) {
		debug("Found Shard: $curr_shard, Public IP: ". $decoded_data->[$curr_shard][0][0][4] .", Port: ". $decoded_data->[$curr_shard][0][1] );

		$hosts{$h}{'host'} = $decoded_data->[$curr_shard][0][0][4];
		$hosts{$h}{'port'} = $decoded_data->[$curr_shard][0][1];

		$curr_shard++;
		$h++;
		$found_host = 1;
	} else {
		last;
	}

}
die "No server:port found in config file!\n" unless $found_host;

=start
# [shard][master][server][ec2_fields]
# [shard][master][port]
# [shard][master][dir]

# [shard][slaves][slave][server][ec2_fields]
# [shard][slaves][slave][port]
# [shard][slaves][slave][dir]

# [shard][haproxy][port]
# [shard][haproxy][server][ec2_fields]
=cut


# Get the list of files to be indexed
my $num_frcr_files = 0;
opendir(DIR, $files_to_index) or die "Cannot open $files_to_index: $!\n";
while ( my $filename = readdir(DIR) ) {

	next if ($filename =~ /^\.$/ or $filename =~ /^\.\.$/);
	push(@files, $files_to_index.$filename);
	debug("Found file: ". $files_to_index.$filename);
	$num_frcr_files++;

}
closedir(DIR);
debug("Found $num_frcr_files frcr files to index");

# Create the threads
my $files_per_thread = int($num_frcr_files/$threads);
my $start = 0;
my $end = $files_per_thread;

for (my $count = 1; $count <= $threads; $count++) {
	$end -= 1 if $threads == 1;
	my $t = threads->new(\&create_thread, $start, $end, $count);
	push(@threads,$t);

	$start = $end+1;
	$end += ($files_per_thread);
	if ($count == $threads-1) {
		$end = scalar(@files)-1;
	}
}
foreach (@threads) {
	my $num = $_->join;
	debug("Done with thread $num\n");
}


# Print out stats
printf("FINAL: Total Files Indexed: $global_num_frcr_indexed/$num_frcr_files (%.2f%%).\n", ($global_num_frcr_indexed/$num_frcr_files)*100);
printf("FINAL: Amount Indexed: %.2f MB\n", $global_amount_of_data_indexed);

exit;

#############################
# create_thread sub routine #
#############################
# args: starting_file_index, ending_file_index, thread_num
sub create_thread {

	my ($start_file_index,$ending_file_index,$thread_num) = @_;
	debug("Starting thread $thread_num");
	debug("Thread $thread_num: indexing files $start_file_index - $ending_file_index");

	# Open each file and index
	my $j = 1;
	my $num_frcr_indexed = 0;
	my $amount_of_data_indexed = 0;
	my $pct_done = 5;

	my $num_to_index = 0;
	foreach ($start_file_index .. $ending_file_index) { $num_to_index++; }

	foreach my $n ($start_file_index .. $ending_file_index) {
		my $file_name = $files[$n];
		if ($j == $h) {
			$j = 1;
		}

		if ($kill_threads) {
			debug("returning thread $thread_num");
			return;

		} else {
			debug("\$kill_threads = $kill_threads");
		}

		add_file($file_name, $hosts{$j}{'host'}, $hosts{$j}{'port'});
		debug("Thread $thread_num: Added file $file_name to ". $hosts{$j}{'host'} .":". $hosts{$j}{'port'});

		$amount_of_data_indexed        += ((-s $file_name) / (1024 * 1024));
		$global_amount_of_data_indexed += ((-s $file_name) / (1024 * 1024));
		$num_frcr_indexed++;
		$global_num_frcr_indexed++;
		$j++;

		debug("\$global_amount_of_data_indexed = $global_amount_of_data_indexed");
		debug("\$global_num_frcr_indexed = $global_num_frcr_indexed");

		if ( (($num_frcr_indexed/$num_frcr_files)*100) >= $pct_done) {
			print "Thread $thread_num: $num_frcr_indexed/$num_to_index ($pct_done%) files have been indexed.\n";
			$pct_done += 5;
		}
	}
}

########################
# add_file sub routine #
########################
# args: file, host, port
sub add_file {
	my ($file,$host,$port) = @_;
	my $counter = 0;
	my $text    = 0;
	my $current_contents;
	my %fields = ();

	# Open the xml file and get the field
	#    values for DOCNO, PARENT, TEXT
	open (XML, $file);
	while (<XML>) {
		next if /<!--/;
		next if /^\n$/;


		if (/===EOD===/) {
			$current_contents =~ s/<\w+>//g;
			$current_contents =~ s/<\/\w+>//g;
			$current_contents =~ s/"//g;
			$current_contents =~ s/'//g;
			$current_contents =~ s/&\w+;/ /g;

			# Escape URI characters
			$current_contents =~ s/%/ /g;
			$current_contents =~ s/ / /g;
			$current_contents =~ s/&/ /g;
			$current_contents =~ s/</ /g;
			$current_contents =~ s/>/ /g;
			$current_contents =~ s/#/ /g;
			$current_contents =~ s/{/ /g;
			$current_contents =~ s/}/ /g;
			$current_contents =~ s/\|/ /g;
			$current_contents =~ s/\\/ /g;
			$current_contents =~ s/\^/ /g;
			$current_contents =~ s/~/ /g;
			$current_contents =~ s/\[/ /g;
			$current_contents =~ s/]/ /g;
			$current_contents =~ s/`/ /g;
			$current_contents =~ s/;/ /g;
			$current_contents =~ s/\// /g;
			$current_contents =~ s/\?/ /g;
			$current_contents =~ s/:/ /g;
			$current_contents =~ s/@/ /g;
			$current_contents =~ s/=/ /g;
			$current_contents =~ s/&/ /g;
			$current_contents =~ s/\$/ /g;

			$fields{$counter}{'text'} = $current_contents;
			$fields{$counter}{'parent'} = $file;

			$counter++;
			$current_contents = '';
			next;
		}

		if (/^Message-ID:\s+(.*)$/) {
			my $msg_id = $1;
			$fields{$counter}{'id'} = $msg_id;
		}

		$current_contents .= $_;
	}
	close (XML);

	# Index docs to Solr
	my $solr = Solr->new(schema=>"./schema.xml",
		port=> "$port",
		url=> "http://$host:$port/solr/update",
		log_dir=> "$log_dir") or die "Cannot connect to Solr\n";

	my $timeout = 5;

	my $num_of_docs = keys %fields;
	debug("Found $num_of_docs Files's\n");

	foreach my $doc (keys %fields) {
		my $d = $fields{$doc};
		$solr->add($d, $timeout) or die "Cannot add field: $!\n";
	}
	$solr->commit() or die "Cannot commit field: $!\n";

}

#####################
# usage sub routine #
#####################
sub usage {
	my $progname = `basename $0`;
	chomp $progname;

	print "
	usage: $progname
	Script for distributing a Solr index accross multiple shards

	NOTE: Script must be run like:
		PERL_SIGNALS=unsafe perl $progname

	-config|c   The location of the configuration file.
	-files|f    The location of the directory containing all of the files to be indexed.
	-log|l      The location of the directory where log files should be placed.
	-threads|t  The number of threads to be created for indexing
	-debug|d    Debug mode
	-help|h|?   Tthis message\n\n";

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

#######################
# Handler sub routine #
#######################
sub Handler {
	print "\n\n CAUGHT SIGINT\n Gracefully Ending threads\n\n";

	$kill_threads = 1;
	sleep(7);

	while (threads->list() > 1) {
		foreach (threads->list()) {
			while ( $_->is_running() ) {
				sleep(0.5);
			}
			$_->join();
		}
	}


	printf("FINAL: Total Files Indexed: $global_num_frcr_indexed/$num_frcr_files (%.2f%%).\n", ($global_num_frcr_indexed/$num_frcr_files)*100);
	printf("FINAL: Amount Indexed: %.2f MB\n", $global_amount_of_data_indexed);
	exit(0);

}
