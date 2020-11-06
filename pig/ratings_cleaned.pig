REGISTER /home/hdoop/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

--Load in ratings data
ratings_raw = LOAD 'movielens/ratings.csv' USING CSVExcelStorage() AS (userId: int, movieId: int, rating: float, timestamp: int);

STORE ratings_raw INTO 'movielens/processed/ratings' USING CSVExcelStorage();
