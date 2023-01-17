-- Chester 2023
-- download files from google drive (not uploaded due to size)
-- hadoop fs -mkdir ./data/credits
-- hadoop fs -mkdir ./data/ratings
-- hadoop fs -mkdir ./data/metadata
-- hadoop fs -put credits.csv ./data/credits
-- hadoop fs -put ratings.csv ./data/ratings
-- hadoop fs -put metadata.csv ./data/metadata
-- hive

create database if not exists movies;
use movies;

create external table if not exists ratings
  (userId INT, movieId INT, rating FLOAT, `timestamp` BIGINT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION '/user/chester/data/ratings/'
  TBLPROPERTIES ("skip.header.line.count"="1") ;

create external table if not exists keywords
  (id BIGINT, keywords String)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION '/user/chester/data/keywords/'
  TBLPROPERTIES ("skip.header.line.count"="1") ;

create external table if not exists links
  (movieId BIGINT, imdbId BIGInt, tmdbId BIGINT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION '/user/chester/data/links/'
  TBLPROPERTIES ("skip.header.line.count"="1") ;

create external table if not exists metadata
  (adult BOOLEAN, belongs_to_collection String, budget BIGINT, genres String, homepage String,
  id BIGINT, imdb_id BIGINT, original_language String, original_title String, overview String,
  popularity FLOAT, poster_path String, production_companies String, production_countries String,
  release_date DATE, revenue BIGINT, runtime FLOAT, spoken_languages String, status String,
  tagline String, title String, video BOOLEAN, vote_average FLOAT, vote_count INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION '/user/chester/data/metadata/'
  TBLPROPERTIES ("skip.header.line.count"="1") ;

create external table if not exists credits
  (casts String, crew String, id BIGINT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION '/user/chester/data/credits/'
  TBLPROPERTIES ("skip.header.line.count"="1") ;

create table all_info
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  AS (
      SELECT    M.adult, M.budget, M.genres, M.id AS ID, M.imdb_id, M.original_language, M.popularity, M.release_date,
                M.revenue, M.runtime, M.spoken_languages, M.status, M.video, M.vote_average, R.rating, `timestamp`,
                C.casts, C.crew
      FROM      metadata M
      JOIN      ratings  R
      ON        R.movieid = M.id
      JOIN      credits C
      ON        R.movieid = C.id
);
