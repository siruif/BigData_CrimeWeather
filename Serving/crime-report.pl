#!/usr/bin/perl -w
# Creates an html table of crime by type for the given ward

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the ward as CGI parameters
my $ward = param('ward');
my $month = param('month');
 
# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server. The first version
# is for our VM, the second is for running on the class cluster
#my $hbase = HBase::JSONRest->new(host => "localhost:8080");
my $hbase = HBase::JSONRest->new(host => "hdp-m.c.mpcs53013-2016.internal:2056");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'crime:kidnapping') gives the value of the
# kidnapping column in the crime family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 'missing';
}

# Query hbase for the ward. For example, if the ward is 1
# the "where" clause of the query will require the key to equal 1
my $records = $hbase->get({
  table => 'siruif_crime',
  where => {
    key_equals => $ward.'_'.$month
  },
});

# There will only be one record for this route, which will be the
# "zeroth" row returned
my $row = @$records[0];

# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my($year, $kidnapping, $concealed_carry_license_violation, $public_peace_violation, 
$intimidation, $prostitution, $liquor_law_violation, $robbery, $burglary, $weapons_violation, 
$human_trafficking, $other_narcotic_violation, $homicide,$obscenity, $other_offense,$criminal_damage, 
$theft, $offense_involving_children, $gambling, $public_indecency, $non_criminal, $arson, $narcotics, 
$sex_offense, $stalking, $interference_with_public_officer, $deceptive_practice, $battery, 
$criminal_trespass, $motor_vehicle_theft, $assault, $crim_sexual_assault)
 =  (cellValue($row, 'crime:year'),
     cellValue($row, 'crime:kidnapping'), 
     cellValue($row, 'crime:concealed_carry_license_violation'),
     cellValue($row, 'crime:public_peace_violation'), 
     cellValue($row, 'crime:intimidation'),
     cellValue($row, 'crime:prostitution'), 
     cellValue($row, 'crime:liquor_law_violation'),
     cellValue($row, 'crime:robbery'), 
     cellValue($row, 'crime:burglary'),
     cellValue($row, 'crime:weapons_violation'), 
     cellValue($row, 'crime:human_trafficking'),
     cellValue($row, 'crime:other_narcotic_violation'), 
     cellValue($row, 'crime:homicide'),
     cellValue($row, 'crime:obscenity'), 
     cellValue($row, 'crime:other_offense'),
     cellValue($row, 'crime:criminal_damage'), 
     cellValue($row, 'crime:theft'),
     cellValue($row, 'crime:offense_involving_children'), 
     cellValue($row, 'crime:gambling'), 
     cellValue($row, 'crime:public_indecency'), 
     cellValue($row, 'crime:on_criminal'), 
     cellValue($row, 'crime:arson'), 
     cellValue($row, 'crime:narcotics'),
     cellValue($row, 'crime:sex_offense'), 
     cellValue($row, 'crime:stalking'), 
     cellValue($row, 'crime:interference_with_public_officer'), 
     cellValue($row, 'crime:deceptive_practice'), 
     cellValue($row, 'crime:battery'), 
     cellValue($row, 'crime:criminal_trespass'), 
     cellValue($row, 'crime:motor_vehicle_theft'),
     cellValue($row, 'crime:assault'), 
     cellValue($row, 'crime:crim_sexual_assault'));

my $num_year = $year - 2001;

# Given the number of years and the total crime frequency, this gives the average crime frequency
sub average_crime_frequency {
    my($crime, $num_year) = @_;
    return $num_year > 0 ? sprintf("%.1f", $crime/$num_year) : "-";
}

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/siruif/table.css',-type=>'text/css'}));

print div({-style=>'margin-left:275px;margin-right:auto;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Crime frequency in Ward ' . $ward . ' in month ' . $month . ' &nbsp;');
print     p({-style=>"bottom-margin:10px"});


print table({-class=>'CSS_Table_Example', -style=>'width:100%;margin:auto;'},
	    Tr([td(['Kidnapping', 'Concealed Carry License Violation', 'Public Peace Violation', 
		'Intimidation', 'Prostitution', 'Liquor Law Violation', 'Robbery', 'Burglary', 
		'Weapons Violation', 'Human Trafficking', 'Other Narcotic Violation', 
		'Homicide', 'Obscenity', 'Other Offense', 'Criminal Damage', 
		'Theft', 'Offense Involving Children', 'Gambling', 'Public Indecency', 
		'Non Criminal', 'Arson', 'Narcotics', 'Sex Offense', 'Stalking', 
		'Interference With Public Officer', 'Deceptive Practice', 'Battery', 
		'Criminal Trespass', 'Motor Vehicle Theft', 'Assault', 'Crim Sexual Assault']),
               	td([ average_crime_frequency($kidnapping, $num_year), 
		average_crime_frequency($concealed_carry_license_violation, $num_year),
                average_crime_frequency($public_peace_violation, $num_year), 
		average_crime_frequency($intimidation, $num_year),
                average_crime_frequency($prostitution, $num_year), 
		average_crime_frequency($liquor_law_violation, $num_year),
                average_crime_frequency($robbery, $num_year), 
		average_crime_frequency($burglary, $num_year),
                average_crime_frequency($weapons_violation, $num_year), 
		average_crime_frequency($human_trafficking, $num_year),
                average_crime_frequency($other_narcotic_violation, $num_year), 
		average_crime_frequency($homicide, $num_year),
                average_crime_frequency($obscenity, $num_year), 
		average_crime_frequency($other_offense, $num_year),
                average_crime_frequency($criminal_damage, $num_year), 
		average_crime_frequency($theft, $num_year),
                average_crime_frequency($offense_involving_children, $num_year), 
		average_crime_frequency($gambling, $num_year),
                average_crime_frequency($public_indecency, $num_year), 
		average_crime_frequency($non_criminal, $num_year),
                average_crime_frequency($arson, $num_year), 
		average_crime_frequency($narcotics, $num_year),
                average_crime_frequency($sex_offense, $num_year), 
		average_crime_frequency($stalking, $num_year),
                average_crime_frequency($interference_with_public_officer, $num_year), 
		average_crime_frequency($deceptive_practice, $num_year),
                average_crime_frequency($battery, $num_year), 
		average_crime_frequency($criminal_trespass, $num_year),
                average_crime_frequency($motor_vehicle_theft, $num_year), 
		average_crime_frequency($assault, $num_year),
                average_crime_frequency($crim_sexual_assault, $num_year)])])),
		p({-style=>"bottom-margin:10px"});


print end_html;