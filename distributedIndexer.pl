#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Long;
use Solr;

## Global Vars ##
my (%solr,%hosts);
my (@files);
my ($config_file,$files_to_index,$log_dir,$debug,$help);


####################
### Main Routine ###
####################

# Get command-line options
GetOptions(
	"config|c=s", \$config_file,
	"files|f=s",  \$files_to_index,
	"log|l=s",    \$log_dir,
	"debug|d",    \$debug,
	"help|h|?",   \$help,
) or usage();

# Check command-line options
usage() if defined($help);
print "Config File Required. See -config for more info.\n\n"       and usage() unless defined($config_file);
print "Files to index are required. See -files for more info.\n\n" and usage() unless defined($files_to_index);
print "Log directory is required. See -log for more info.\n\n"     and usage() unless defined($log_dir);
print "Config File does not exist.\n\n"                            and usage() unless -e $config_file;
print "Directory with files to index does not exist.\n\n"          and usage() unless -d $files_to_index;
print "Specified log directory does not exist.\n\n"                and usage() unless -d $log_dir;


# Debug command-line options
debug("Config File: $config_file");
debug("Directory with Files: $files_to_index");
debug("Log Directory: $log_dir");


# Get the list of config files
open(CON, $config_file) or die "Cannot open $config_file: $!\n";
my $h = 1;
my $found_host = 0;
while (<CON>) {
	next if /^#/;

	if (/^(\d+\.\d+\.\d+\.\d+):(\d+)$/) {
		$hosts{$h}{'host'} = $1;
		$hosts{$h}{'port'} = $2;
		debug("Found Solr host: ". $hosts{$h}{'host'} .":". $hosts{$h}{'port'});
		$h++;
		$found_host = 1;
	}
}
close(CON) or die "Cannot close $config_file: $!\n";
die "No server:port found in config file!\n" unless $found_host;


# Get the list of files to be indexed
opendir(DIR, $files_to_index) or die "Cannot open $files_to_index: $!\n";
while ( my $filename = readdir(DIR) ) {
	push(@files, $files_to_index.$filename) unless ($filename =~ /^\.$/ or $filename =~ /^\.\.$/);
	debug("Found file: ". $files_to_index.$filename);
}
closedir(DIR);

# Open each file and index
my $j = 1;
foreach my $file_name (@files) {
	if ($j == $h) {
		$j = 1;
	}

	add_file($file_name, $hosts{$j}{'host'}, $hosts{$j}{'port'});
	debug("Adding file $file_name to ". $hosts{$j}{'host'} .":". $hosts{$j}{'port'});
	$j++;
}

exit;


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

		if (/<DOC>/) {
			$current_contents = '';
			next;
		}

		if (/<\/DOC>/) {
			$counter++;
			next;
		}

		if (/<DOCNO>(.*)<\/DOCNO>/) {
			my $docno = $1;
			$docno =~ s/^\s+//;
			$docno =~ s/\s+$//;

			$fields{$counter}{'id'} = $docno;
			next;
		}

		if (/<PARENT>(.*)<\/PARENT>/) {
			my $parent = $1;
			$parent =~ s/^\s+//;
			$parent =~ s/\s+$//;

			$fields{$counter}{'parent'} = $parent;
			next;
		}

		if (/^<TEXT>/) {
			$text = 1;
			next;
		}

		if (/^<DOCNO>|^<!--|^\n$/) {
			next;
		}

		if (/^<\w+>(.*)<\/\w+>$/) {
			$current_contents .= $1."\n";
			next;
		}

		if (/<\/TEXT>/) {
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
			$text = 0;
			next;
		}

		$current_contents .= $_ if $text == 1;
	}
	close (XML);

	# Index docs to Solr
	my $solr = Solr->new(schema=>"./schema.xml",
		port=> "$port",
		url=> "http://$host:$port/solr/update",
		log_dir=> "$log_dir") or die "Cannot connect to Solr\n";

	my $timeout = 5;

	my $num_of_docs = keys %fields;
	print "Found $num_of_docs DOCNO's\n";

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

	-config|c   The location of the configuration file.
	-files|f    The location of the directory containing all of the files to be indexed.
	-log|l      The location of the directory where log files should be placed.
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
