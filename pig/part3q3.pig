REGISTER /home/hdoop/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

--Load in ratings data
ratings_raw = LOAD 'movielens/ratings.csv' USING CSVExcelStorage() AS (userId: int, movieId: int, rating: float, timestamp: int);

--Group data by user
grouped_user = GROUP ratings_raw BY userId;
avg_ratings = FOREACH grouped_user GENERATE group AS userId, AVG(ratings_raw.rating) AS ratings_avg, COUNT(ratings_raw.rating) AS ratings_cnt;
sorted = ORDER avg_ratings BY ratings_avg DESC, ratings_cnt DESC;

out = LIMIT sorted 20;
DUMP out;
