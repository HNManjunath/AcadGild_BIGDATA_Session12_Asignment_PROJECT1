REGISTER '/home/acadgild/manjunath/PigLib/piggybank.jar';

crime_data_File = LOAD '/home/acadgild/manjunath/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

Selected_data = FOREACH crime_data_File GENERATE (chararray)$2 as dateofArrest, (chararray)$8 as Arrest;

filter_selected_data = FILTER Selected_data BY (dateofArrest is not null) AND (Arrest == 'true');


FetchDateTime = FOREACH filter_selected_data GENERATE ToDate(SUBSTRING(dateofArrest,0,19),'MM/dd/yyyy hh:mm:ss') as Date_time,Arrest;

Arrest_by_date = FOREACH FetchDateTime GENERATE GetMonth(Date_time) as Month,GetYear(Date_time) as Year,Arrest;

total_no_of_Arrest = FILTER Arrest_by_date BY (Month>9 AND Year == 2014) OR (Month<11 and Year == 2015);

group_by_Arrest = GROUP total_no_of_Arrest ALL;

count_no_of_arrest = FOREACH group_by_Arrest GENERATE COUNT(total_no_of_Arrest.Arrest) as total_no_Of_arrests;

dump count_no_of_arrest ;