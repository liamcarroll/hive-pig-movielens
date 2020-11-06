REGISTER /home/hdoop/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

--Load in ratings data
tags_raw = LOAD 'movielens/tags.csv' USING CSVExcelStorage() AS (userId: int, movieId: int, tag: chararray, timestamp: int);

STORE tags_raw INTO 'movielens/processed/x' USING CSVExcelStorage();
