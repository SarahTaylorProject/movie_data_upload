DROP VIEW IF EXISTS combined.vw_unique_movie_ids CASCADE;
CREATE VIEW combined.vw_unique_movie_ids AS
SELECT movie_id, movie_title, parent_id, max(running_time) as running_time_max
FROM combined.movie_xml_extract
GROUP BY movie_id, movie_title, parent_id
ORDER BY movie_title, movie_id, parent_id;

SELECT * FROM combined.vw_unique_movie_ids;

