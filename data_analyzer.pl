#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Long;
use Spreadsheet::WriteExcel;

## Global Vars ##
my (%query_data);
my ($query_file,$debug,$help);


####################
### Main Routine ###
####################

# Get command-line options
GetOptions(
	"query_file|q=s",  \$query_file,
	"debug|d",         \$debug,
	"help|?",          \$help,
) or usage();

# Check command-line options
usage() if defined($help);
print "Query File not given. See -query_file below:\n"   and usage() unless defined($query_file);
print "Query File does not exist.\n"                     and usage() unless -e $query_file;


# Debug command-line options
debug("Query File:  $query_file");


# Get the query data from the file
my $k = 1;
open(QRY, "$query_file") or die "Cannot open $query_file: $!\n";
while (<QRY>) {

	chomp;
	my $curr_line = $_;
	debug("Current line is: ***$_***");

	if ($curr_line = /^Server Hostname:\s+(.*)$/) {
		$query_data{$k}{'hostname'} = $1;
	}
	if ($curr_line = /^Server Port:\s+(\d+)$/) {
		$query_data{$k}{'port'} = $1;
	}
	if ($curr_line = /^Document Path:\s+(.*)q=(.*)$/) {
		$query_data{$k}{'path'}  = $1."q=".$2;
		$query_data{$k}{'query'} = $2;
	}
	if ($curr_line = /^Document Length:\s+(\d+) bytes$/) {
		$query_data{$k}{'doc_length_bytes'} = $1;
	}
	if ($curr_line = /^Concurrency Level:\s+(\d+)$/) {
		$query_data{$k}{'concurrency'} = $1;
	}
	if ($curr_line = /^Time taken for tests:\s+(\d+\.\d+) seconds$/) {
		$query_data{$k}{'total_time_sec_for_all_test'} = $1;
	}
	if ($curr_line = /^Complete requests:\s+(\d+)$/) {
		$query_data{$k}{'complete_requests'} = $1;
	}
	if ($curr_line = /^Failed requests:\s+(\d+)$/) {
		$query_data{$k}{'failed_requests'} = $1;
	}
	if ($curr_line = /^Write errors:\s+(\d+)$/) {
		$query_data{$k}{'write_errors'} = $1;
	}
	if ($curr_line = /^Total transferred:\s+(\d+) bytes$/) {
		$query_data{$k}{'total_bytes_transfered'} = $1;
	}
	if ($curr_line = /^HTML transferred:\s+(\d+) bytes$/) {
		$query_data{$k}{'html_bytes_transfered'} = $1;
	}
	if ($curr_line = /^Requests per second:\s+(\d+\.\d+) \[#\/sec\] \(mean\)$/) {
		$query_data{$k}{'avg_request_per_sec'} = $1;
	}
	if ($curr_line = /^Time per request:\s+(\d+\.\d+) \[ms\] \(mean\)$/) {
		$query_data{$k}{'avg_time_per_req'} = $1;
	}
	if ($curr_line = /^Time per request:\s+(\d+\.\d+) \[ms\] \(mean, across all concurrent requests\)$/) {
		$query_data{$k}{'avg_time_per_req_all_concurrent'} = $1;
	}
	if ($curr_line = /^Transfer rate:\s+(\d+.\d+) \[Kbytes\/sec\] received$/) {
		$query_data{$k}{'transfer_rate_kb_per_sec'} = $1;
	}
	if ($curr_line = /^Connect:\s+(\d+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+)\s+(\d+)$/) {
		$query_data{$k}{'connect_time_min'}    = $1;
		$query_data{$k}{'connect_time_mean'}   = $2;
		$query_data{$k}{'connect_time_sd'}     = $3;
		$query_data{$k}{'connect_time_median'} = $4;
		$query_data{$k}{'connect_time_max'}    = $5;
	}
	if ($curr_line = /^Processing:\s+(\d+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+)\s+(\d+)$/) {
		$query_data{$k}{'processing_time_min'}    = $1;
		$query_data{$k}{'processing_time_mean'}   = $2;
		$query_data{$k}{'processing_time_sd'}     = $3;
		$query_data{$k}{'processing_time_median'} = $4;
		$query_data{$k}{'processing_time_max'}    = $5;
	}
	if ($curr_line = /^Waiting:\s+(\d+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+)\s+(\d+)$/) {
		$query_data{$k}{'waiting_time_min'}    = $1;
		$query_data{$k}{'waiting_time_mean'}   = $2;
		$query_data{$k}{'waiting_time_sd'}     = $3;
		$query_data{$k}{'waiting_time_median'} = $4;
		$query_data{$k}{'waiting_time_max'}    = $5;
	}
	if ($curr_line = /^Total:\s+(\d+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+)\s+(\d+)$/) {
		$query_data{$k}{'total_time_min'}    = $1;
		$query_data{$k}{'total_time_mean'}   = $2;
		$query_data{$k}{'total_time_sd'}     = $3;
		$query_data{$k}{'total_time_median'} = $4;
		$query_data{$k}{'total_time_max'}    = $5;
		$k++;
	}

}
close(QRY) or die "Cannot close $query_file: $!\n";


=start
# Debug - Spit out contents of %query_data
foreach my $key (sort keys %query_data) {
	foreach my $key2 (sort keys %{ $query_data{$key} }) {
		debug("\$query_data{$key}{$key2} = ". $query_data{$key}{$key2});
	}
}
=cut



# Create the spreadsheet
my ($xls_name,$workbook,$worksheet,$header_format,$pct_format,$generic_format);
$xls_name = 'query_stats.xls';
$workbook = Spreadsheet::WriteExcel->new("$xls_name");

# Create the worksheet
$worksheet = $workbook->add_worksheet();

# Adjust the width of the columns
$worksheet->set_column('A:B', 45);
$worksheet->set_column('C:L', 25);
$worksheet->set_column('M:M', 40);
$worksheet->set_column('N:N', 25);
$worksheet->set_column('O:R', 50);

# Create the header format
$header_format = $workbook->add_format();
$header_format->set_align('center');
$header_format->set_bg_color('gray');
$header_format->set_bold();
$header_format->set_border();

# Create the percent format
$pct_format = $workbook->add_format();
$pct_format->set_align('center');
$pct_format->set_bg_color('white');
$pct_format->set_border();
$pct_format->set_num_format('.00%');

# Create a generic format
$generic_format = $workbook->add_format();
$generic_format->set_align('center');
$generic_format->set_bg_color('white');
$generic_format->set_border();

# Add the headers to the spreadsheet
$worksheet->write('A1', 'Query',                                        $header_format);
$worksheet->write('B1', 'Host',                                         $header_format);
$worksheet->write('C1', 'Port',                                         $header_format);
$worksheet->write('D1', 'Total Time For Tests (sec)',                   $header_format);
$worksheet->write('E1', 'Concurrency',                                  $header_format);
$worksheet->write('F1', 'Complete Requests',                            $header_format);
$worksheet->write('G1', 'Failed Requests',                              $header_format);
$worksheet->write('H1', 'Write Errors',                                 $header_format);
$worksheet->write('I1', 'Total Transfered (bytes)',                     $header_format);
$worksheet->write('J1', 'HTML Transfered (bytes)',                      $header_format);
$worksheet->write('K1', 'Requests per Second (mean)',                   $header_format);
$worksheet->write('L1', 'Time per Request (mean ms)',                    $header_format);
$worksheet->write('M1', 'Time per Request (mean ms, all concurrency)',   $header_format);
$worksheet->write('N1', 'Transfer Rate (Kbytes/sec)',                   $header_format);
$worksheet->write('O1', 'Connect Time (min, mean, std, median, max in ms)',      $header_format);
$worksheet->write('P1', 'Processing Time (min, mean, std, median, max in ms)',   $header_format);
$worksheet->write('Q1', 'Waiting Time (min, mean, std, median, max in ms)',      $header_format);
$worksheet->write('R1', 'Total Time (min, mean, std, median, max in ms)',        $header_format);

# Populate the spreadsheet with %query_data
foreach my $key (sort keys %query_data) {
	my $c = $key+1;
	my $con_time = $query_data{$key}{'connect_time_min'}.", ".$query_data{$key}{'connect_time_mean'}.", ".$query_data{$key}{'connect_time_sd'}.", ".$query_data{$key}{'connect_time_median'}.", ".$query_data{$key}{'connect_time_max'};
	my $proc_time = $query_data{$key}{'processing_time_min'}.", ".$query_data{$key}{'processing_time_mean'}.", ".$query_data{$key}{'processing_time_sd'}.", ".$query_data{$key}{'processing_time_median'}.", ".$query_data{$key}{'processing_time_max'};
	my $wait_time = $query_data{$key}{'waiting_time_min'}.", ".$query_data{$key}{'waiting_time_mean'}.", ".$query_data{$key}{'waiting_time_sd'}.", ".$query_data{$key}{'waiting_time_median'}.", ".$query_data{$key}{'waiting_time_max'};
	my $tot_time = $query_data{$key}{'total_time_min'}.", ".$query_data{$key}{'total_time_mean'}.", ".$query_data{$key}{'total_time_sd'}.", ".$query_data{$key}{'total_time_median'}.", ".$query_data{$key}{'total_time_max'};

	$worksheet->write('A'.$c,         $query_data{$key}{'query'},                            $generic_format);
	$worksheet->write('B'.$c,         $query_data{$key}{'hostname'},                         $generic_format);
	$worksheet->write('C'.$c,         $query_data{$key}{'port'},                             $generic_format);
	$worksheet->write('D'.$c,         $query_data{$key}{'total_time_sec_for_all_test'},      $generic_format);
	$worksheet->write('E'.$c,         $query_data{$key}{'concurrency'},                      $generic_format);
	$worksheet->write('F'.$c,         $query_data{$key}{'complete_requests'},                $generic_format);
	$worksheet->write('G'.$c,         $query_data{$key}{'failed_requests'},                  $generic_format);
	$worksheet->write('H'.$c,         $query_data{$key}{'write_errors'},                     $generic_format);
	$worksheet->write('I'.$c,         $query_data{$key}{'total_bytes_transfered'},           $generic_format);
	$worksheet->write('J'.$c,         $query_data{$key}{'html_bytes_transfered'},            $generic_format);
	$worksheet->write('K'.$c,         $query_data{$key}{'avg_request_per_sec'},              $generic_format);
	$worksheet->write('L'.$c,         $query_data{$key}{'avg_time_per_req'},                 $generic_format);
	$worksheet->write('M'.$c,         $query_data{$key}{'avg_time_per_req_all_concurrent'},  $generic_format);
	$worksheet->write('N'.$c,         $query_data{$key}{'transfer_rate_kb_per_sec'},         $generic_format);
	$worksheet->write('O'.$c,         $con_time,                                             $generic_format);
	$worksheet->write('P'.$c,         $proc_time,                                            $generic_format);
	$worksheet->write('Q'.$c,         $wait_time,                                            $generic_format);
	$worksheet->write('R'.$c,         $tot_time,                                             $generic_format);
}

exit;


#####################
# usage sub routine #
#####################
sub usage {
	my $progname = `basename $0`;
	chomp $progname;

	print "
	usage: $progname
	Script for performing analyzing the data provided from ApacheBench (ab)
		The script will create a spreadsheet with all the data and also
		create graphs of the data.

	-query_file|q  The file which has the ab results from the queries
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

