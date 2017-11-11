REGISTER '/home/acadgild/manjunath/PigLib/piggybank.jar';

crime_data_File = LOAD '/home/acadgild/manjunath/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

Selected_data = FOREACH crime_data_File GENERATE (chararray)$14 as FBI_Code, (chararray)$1 as Case_Number;

filter_selected_data = FILTER Selected_data BY (FBI_Code == '32') AND (Case_Number is not null);

group_by_fbicode = GROUP filter_selected_data ALL ;

count_no_of_cases_investigated_for_fbi_code32 = foreach group_by_fbicode GENERATE COUNT(filter_selected_data.Case_Number) as NoOfCasesInvestigatedforFBIcode32;

dump count_no_of_cases_investigated_for_fbi_code32 ;