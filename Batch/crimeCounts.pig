CRIME = LOAD '/siruif/final/pigData/part*' USING PigStorage(',')
AS (case_num, month:int, day:int, year:int,
  block, description, ward:int);

CRIME_EVENT = FOREACH CRIME GENERATE  
	ward, 
	year,
	month,
	(description == 'KIDNAPPING' ? 1 : 0) AS KIDNAPPING,
	(description == 'CONCEALED CARRY LICENSE VIOLATION' ? 1 : 0) AS CONCEALED_CARRY_LICENSE_VIOLATION,
	(description == 'PUBLIC PEACE VIOLATION' ? 1 : 0) AS PUBLIC_PEACE_VIOLATION,
	(description == 'INTIMIDATION' ? 1 : 0) AS INTIMIDATION,
	(description == 'PROSTITUTION' ? 1 : 0) AS PROSTITUTION,
	(description == 'LIQUOR LAW VIOLATION' ? 1 : 0) AS LIQUOR_LAW_VIOLATION,
	(description == 'ROBBERY' ? 1 : 0) AS ROBBERY,
	(description == 'BURGLARY' ? 1 : 0) AS BURGLARY,
	(description == 'WEAPONS VIOLATION' ? 1 : 0) AS WEAPONS_VIOLATION,
	(description == 'HUMAN TRAFFICKING' ? 1 : 0) AS HUMAN_TRAFFICKING,
	(description == 'OTHER NARCOTIC VIOLATION' ? 1 : 0) AS OTHER_NARCOTIC_VIOLATION,
	(description == 'HOMICIDE' ? 1 : 0) AS HOMICIDE,
	(description == 'OBSCENITY' ? 1 : 0) AS OBSCENITY,
	(description == 'OTHER OFFENSE' ? 1 : 0) AS OTHER_OFFENSE,
	(description == 'CRIMINAL DAMAGE' ? 1 : 0) AS CRIMINAL_DAMAGE,
	(description == 'THEFT' ? 1 : 0) AS THEFT,
	(description == 'OFFENSE INVOLVING CHILDREN' ? 1 : 0) AS OFFENSE_INVOLVING_CHILDREN,
	(description == 'GAMBLING' ? 1 : 0) AS GAMBLING,
	(description == 'PUBLIC INDECENCY' ? 1 : 0) AS PUBLIC_INDECENCY,
	(description MATCHES '*NON-CRIMINAL*' ? 1 : 0) AS NON_CRIMINAL,
	(description == 'ARSON' ? 1 : 0) AS ARSON,
	(description == 'NARCOTICS' ? 1 : 0) AS NARCOTICS,
	(description == 'SEX OFFENSE' ? 1 : 0) AS SEX_OFFENSE,
	(description == 'STALKING' ? 1 : 0) AS STALKING,
	(description == 'INTERFERENCE WITH PUBLIC OFFICER' ? 1 : 0) AS INTERFERENCE_WITH_PUBLIC_OFFICER,
	(description == 'DECEPTIVE PRACTICE' ? 1 : 0) AS DECEPTIVE_PRACTICE,
	(description == 'BATTERY' ? 1 : 0) AS BATTERY,
	(description == 'CRIMINAL TRESPASS' ? 1 : 0) AS CRIMINAL_TRESPASS,
	(description == 'MOTOR VEHICLE THEFT' ? 1 : 0) AS MOTOR_VEHICLE_THEFT,
	(description == 'ASSAULT' ? 1 : 0) AS ASSAULT,
	(description == 'CRIM SEXUAL ASSAULT' ? 1 : 0) AS CRIM_SEXUAL_ASSAULT;

CRIME_BY_WARD = GROUP CRIME_EVENT BY (ward,month);

SUMMED_CRIME_BY_WARD = FOREACH CRIME_BY_WARD 
  GENERATE StringConcat(group.ward,'_',  group.month) AS ward_month, 
		MAX($1.year) AS year,		
		SUM($1.KIDNAPPING) AS kidnapping,
		SUM($1.CONCEALED_CARRY_LICENSE_VIOLATION) AS concealed_carry_license_violation,
		SUM($1.PUBLIC_PEACE_VIOLATION) AS public_peace_violation,
		SUM($1.INTIMIDATION) AS intimidation,
		SUM($1.PROSTITUTION) AS prostitution,
		SUM($1.LIQUOR_LAW_VIOLATION) AS liquor_law_violation,
		SUM($1.ROBBERY) AS robbery,
		SUM($1.BURGLARY) AS burglary,
		SUM($1.WEAPONS_VIOLATION) AS weapons_violation,
		SUM($1.HUMAN_TRAFFICKING) AS human_trafficking,
		SUM($1.OTHER_NARCOTIC_VIOLATION) AS other_narcotic_violation,
		SUM($1.HOMICIDE) AS homicide,
		SUM($1.OBSCENITY) AS obscenity,
		SUM($1.OTHER_OFFENSE) AS other_offense,
		SUM($1.CRIMINAL_DAMAGE) AS criminal_damage,
		SUM($1.THEFT) AS theft,
		SUM($1.OFFENSE_INVOLVING_CHILDREN) AS offense_involving_children,
		SUM($1.GAMBLING) AS gambling,
		SUM($1.PUBLIC_INDECENCY) AS public_indecency,
		SUM($1.NON_CRIMINAL) AS non_criminal,
		SUM($1.ARSON) AS arson,
		SUM($1.NARCOTICS) AS narcotics,
		SUM($1.SEX_OFFENSE) AS sex_offense,
		SUM($1.STALKING) AS stalking,
		SUM($1.INTERFERENCE_WITH_PUBLIC_OFFICER) AS interference_with_public_officer,
		SUM($1.DECEPTIVE_PRACTICE) AS deceptive_practice,
		SUM($1.BATTERY) AS battery,
		SUM($1.CRIMINAL_TRESPASS) AS criminal_trespass,
		SUM($1.MOTOR_VEHICLE_THEFT) AS motor_vehicle_theft,
		SUM($1.ASSAULT) AS assault,
		SUM($1.CRIM_SEXUAL_ASSAULT) AS crim_sexual_assault;


STORE SUMMED_CRIME_BY_WARD INTO 'hbase://siruif_crime'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
   'crime:year, crime:kidnapping, crime:concealed_carry_license_violation, crime:public_peace_violation, crime:intimidation, crime:prostitution, crime:liquor_law_violation, crime:robbery, crime:burglary, crime:weapons_violation, crime:human_trafficking, crime:other_narcotic_violation, crime:homicide, crime:obscenity, crime:other_offense,crime:criminal_damage, crime:theft, crime:offense_involving_children, crime:gambling, crime:public_indecency, crime:non_criminal, crime:arson, crime:narcotics, crime:sex_offense, crime:stalking, crime:interference_with_public_officer, crime:deceptive_practice, crime:battery, crime:criminal_trespass, crime:motor_vehicle_theft, crime:assault, crime:crim_sexual_assault');


