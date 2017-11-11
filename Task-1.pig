REGISTER '/home/acadgild/manjunath/PigLib/piggybank.jar';

crime_data_File = LOAD '/home/acadgild/manjunath/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

Selected_data = FOREACH crime_data_File GENERATE (chararray)$14 as FBI_Code, (chararray)$1 as Case_Number;

filter_selected_data = FILTER Selected_data BY (FBI_Code is not null) AND (Case_Number is not null);

group_by_fbicode = GROUP filter_selected_data by FBI_Code ;

count_no_of_cases = foreach group_by_fbicode GENERATE group as FBI_Code, COUNT(filter_selected_data.Case_Number) as NoOfCasesInvestigated;

dump count_no_of_cases ;