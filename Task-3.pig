REGISTER '/home/acadgild/manjunath/PigLib/piggybank.jar';

crime_data_File = LOAD '/home/acadgild/manjunath/Crimes_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

Selected_data = FOREACH crime_data_File GENERATE (int)$11 as district, (chararray)$5 as Primary_type, (chararray)$8 as Arrest;

filter_selected_data = FILTER Selected_data BY (district is not null) AND (Primary_type == 'THEFT') AND (Arrest == 'true');

group_by_fbicode = GROUP filter_selected_data BY district;

count_no_of_theft_arrest = FOREACH group_by_fbicode GENERATE group, COUNT(filter_selected_data.Arrest) as no_Of_theft_arrests;

dump count_no_of_theft_arrest ;