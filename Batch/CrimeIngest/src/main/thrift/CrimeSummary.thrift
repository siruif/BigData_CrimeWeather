namespace java edu.uchicago.mpcs53013.CrimeSummary

struct CrimeSummary {
	1: required string case_num;
	2: required byte month;
	3: required byte day;
	4: required i16 year;
	5: required string block;
	6: required string description;
	7: required i32 ward;
}