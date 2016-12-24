REGISTER /usr/local/mpcs53013/elephant-bird-core-4.14.jar;
REGISTER /usr/local/mpcs53013/elephant-bird-pig-4.14.jar;
REGISTER /usr/local/mpcs53013/elephant-bird-hadoop-compat-4.14.jar;
REGISTER /usr/local/mpcs53013/libthrift-0.9.0.jar;
register /home/mpcs53013/workspace/FinalProject/Batch/CrimeIngest/target/uber-CrimeIngest-0.0.1-SNAPSHOT.jar;
REGISTER /usr/local/mpcs53013/piggybank.jar;

DEFINE WSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('edu.uchicago.mpcs53013.CrimeSummary.CrimeSummary');

RAW_DATA = LOAD '/siruif/final/crime/*' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
CRIME_SUMMARY = FOREACH RAW_DATA GENERATE FLATTEN(WSThriftBytesToTuple(value));

STORE CRIME_SUMMARY into '/siruif/final/pigData' Using PigStorage(',');
