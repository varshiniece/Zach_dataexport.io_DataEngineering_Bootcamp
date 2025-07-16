-- Creating DDL for actors table
-- Step1: Struct array for films

CREATE TYPE films_struct AS (
    film TEXT,
    votes INT,
    rating NUMERIC,
    filmid TEXT,
    year INT
);

-- Step 2: Creating quality class array
CREATE TYPE quality_class AS 
	ENUM ('star','good','average','bad');

-- Step 3: Creating table for actors
CREATE TABLE actors(
		actorid TEXT,
		actor TEXT,
		current_year INTEGER,
		films films_struct[],
		quality_class quality_class,
		is_active BOOLEAN,
		PRIMARY KEY (actorid,current_year)	
);
