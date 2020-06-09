-- Sarah 9/6/2020, after Zoom chat with June
-- This SQL makes a TABLE from screening records, which excludes the superfluous superseded records
-- It does this by first creating a view of screening records which contains a record_rank field
-- Records with the same movie_id, theater_id, and main_show_date are grouped and ranked
-- The record with the LARGEST sub directory will be ranked highest
-- By 'LARGEST' we mean a sub directory for the source XML file, with the highest date
-- The query relies on the existence of a view of all screening input records (here name combined.vw_screening_records_with_rank)
-- It creates (and overwrites if necessary) a table of de-duplicated screening records, named combined.screenings
-- Note that this table is purposefully minimalist: only the movie_id, theater_id, main_show_date, show_time_count, and country_code_guess
-- Beware that this query will still be likely to take some time to run, and create a large table

DROP VIEW IF EXISTS combined.vw_screening_records_with_rank CASCADE;
CREATE VIEW combined.vw_screening_records_with_rank AS
SELECT movie_id, theater_id, main_show_date, right(sub_directory, 30) as sub_directory_part, file_name,
show_time_count, country_code_guess,
rank() OVER (PARTITION BY movie_id, theater_id, main_show_date ORDER BY sub_directory DESC) as record_rank
FROM combined.screening_xml_extract
ORDER BY record_rank DESC;

DROP TABLE IF EXISTS combined.screenings CASCADE;
SELECT movie_id, theater_id, main_show_date, 
show_time_count, country_code_guess
INTO combined.screenings
FROM combined.vw_screening_records_with_rank
WHERE record_rank = 1;

SELECT * FROM combined.screenings LIMIT 100;