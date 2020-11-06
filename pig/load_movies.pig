REGISTER /home/hdoop/pig-0.17.0/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

--Load the movies csv file
movies_raw = LOAD 'movielens/movies.csv' USING CSVExcelStorage() AS (movieId: int, title: chararray, genres: chararray);

--Process the columns
movies_clean = FOREACH movies_raw GENERATE movieId, SUBSTRING($1, 0,(int)SIZE($1) - 7) AS title, REGEX_EXTRACT($1, '([0-9][0-9][0-9][0-9])', 1) AS year,  genres;

--Load the links csv file
links_raw = LOAD 'movielens/links.csv' USING CSVExcelStorage() AS (movieId: int, imdbId: int, tmdbId: int);

--Join the two to make the new movies table
joined = JOIN movies_clean BY movieId, links_raw BY movieId;
movies_final = FOREACH joined GENERATE $0 AS movieId, $1 AS title, $2 AS year, $5 AS imdbId, $6 AS tmdbId, $3 AS genres;
STORE movies_final INTO 'movielens/processed' USING CSVExcelStorage;
