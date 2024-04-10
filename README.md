// Name: Omar Mohammad
// Student ID: 40162541
// Github repo link of the solution: https://github.com/OmarTheProgrammerr/SOEN363_A3.git

// Regarding the first part ( the CSV files ), you can find them under CSVFiles folder.

// Script to create database in Neo4J -> Question 2 :

// Name: Omar Mohammad
// Student ID: 40162541

// I'm Loading and creating Movie nodes here
LOAD CSV WITH HEADERS FROM 'file:///movie.csv' AS movieRow
MERGE (m:Movie {imdb_id: toInteger(movieRow.imdb_id)})
SET m.title = movieRow.title,
m.description = movieRow.description,
m.rating = toFloat(movieRow.rating),
m.release_year = toInteger(movieRow.release_year),
m.runtime = toInteger(movieRow.runtime);

// Load Languages and create Language nodes here
LOAD CSV WITH HEADERS FROM 'file:///language.csv' AS languageRow
MERGE (l:Language {language_id: toInteger(languageRow.language_id)})
SET l.language_name = languageRow.language_name;

// Establish relationships between Movie and Language nodes using movie_language.csv
LOAD CSV WITH HEADERS FROM 'file:///movie_language.csv' AS mlRow
MATCH (m:Movie {imdb_id: toInteger(mlRow.imdb_id)})
MATCH (l:Language {language_id: toInteger(mlRow.language_id)})
MERGE (m)-[:HAS_LANGUAGE]->(l);

// Set languages attribute in Movie nodes to an array of associated language names
MATCH (m:Movie)-[:HAS_LANGUAGE]->(l:Language)
WITH m, collect(l.language_name) AS languages
SET m.languages = languages;

// Load Genres and create nodes
LOAD CSV WITH HEADERS FROM 'file:///genre.csv' AS genreRow
MERGE (g:Genre {genre_id: toInteger(genreRow.genre_id)})
SET g.genre_name = genreRow.genre_name;

// Establish relationships between Movie and Genre nodes using movie_genre.csv
LOAD CSV WITH HEADERS FROM 'file:///movie_genre.csv' AS mgRow
MATCH (m:Movie {imdb_id: toInteger(mgRow.imdb_id)})
MATCH (g:Genre {genre_id: toInteger(mgRow.genre_id)})
MERGE (m)-[:HAS_GENRE]->(g);

// Set genres attribute in Movie nodes to an array of associated genre names
MATCH (m:Movie)-[:HAS_GENRE]->(g:Genre)
WITH m, collect(g.genre_name) AS genres
SET m.genres = genres;

// Load Actors and create nodes
LOAD CSV WITH HEADERS FROM 'file:///actor.csv' AS actorRow
MERGE (a:Actor {actor_id: toInteger(actorRow.actor_id)})
SET a.first_name = actorRow.first_name,
a.last_name = actorRow.last_name;

// Establish relationships between Actor and Movie nodes using movie_actor.csv
LOAD CSV WITH HEADERS FROM 'file:///movie_actor.csv' AS maRow
MATCH (m:Movie {imdb_id: toInteger(maRow.imdb_id)})
MATCH (a:Actor {actor_id: toInteger(maRow.actor_id)})
MERGE (a)-[act:ACTED_IN]->(m)
SET act.character_name = maRow.character_name;

// Load Countries and create nodes
LOAD CSV WITH HEADERS FROM 'file:///country.csv' AS countryRow
MERGE (c:Country {country_name: countryRow.country_name})
SET c.country_code = countryRow.country_code;

// Establish relationships between Movie and Country nodes using movie_countries.csv
LOAD CSV WITH HEADERS FROM 'file:///movie_country.csv' AS mcRow
MATCH (m:Movie {imdb_id: toInteger(mcRow.imdb_id)})
MATCH (c:Country {country_name: mcRow.country_name})
MERGE (m)-[:AVAILABLE_IN]->(c);

// Load Keywords and create nodes
LOAD CSV WITH HEADERS FROM 'file:///keyword.csv' AS keywordRow
MERGE (k:Keyword {keyword_id: toInteger(keywordRow.keyword_id)})
SET k.keyword_name = keywordRow.keyword_name;

// Establish relationships between Movie and Keyword nodes using movie_keywords.csv
LOAD CSV WITH HEADERS FROM 'file:///movie_keyword.csv' AS mkRow
MATCH (m:Movie {imdb_id: toInteger(mkRow.imdb_id)})
MATCH (k:Keyword {keyword_id: toInteger(mkRow.keyword_id)})
MERGE (m)-[:HAS_KEYWORD]->(k);

// Remove Genre nodes and relationships
MATCH (g:Genre)
DETACH DELETE g;

// Remove Language nodes and relationships
MATCH (l:Language)
DETACH DELETE l;

// Quries -> Question 3:

// A) Find all movies that are played by a sample actor.

MATCH (a:Actor {first_name: 'Tom', last_name: 'Cruise'})-[:ACTED_IN]->(m:Movie)
RETURN m.imdb_id, m.title AS movie_title;

// B) Find all movies that are released after the year 2000 and has a rating of at least 5.

MATCH (m:Movie)
WHERE m.release_year > 2000 AND m.rating >= 5
RETURN m.title AS movie_title, m.release_year AS release_year, m.rating AS rating;

// C) Find all movies that share two keywords of your choice. Make sure your query returns more than one movie.

MATCH (m:Movie)-[:HAS_KEYWORD]->(k1:Keyword {keyword_name: "Drama"}),
(m:Movie)-[:HAS_KEYWORD]->(k2:Keyword {keyword_name: "Action"})
RETURN m.title AS movie_title;

// D) Find top 2 movies with largest number of keywords. Here, I'm doing it in descinding order

MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword)
WITH m, count(k) AS keywords_count
ORDER BY keywords_count DESC
RETURN m.title AS movie_title, keywords_count
LIMIT 2;

// E) Find top 10 movies (ordered by rating) in a language of your choice.

MATCH (m:Movie)
WHERE "English" IN m.languages
RETURN m.title AS Movie, m.rating AS Rating
ORDER BY m.rating DESC
LIMIT 10

// F) Build full text search index to query movie plots.

CREATE FULLTEXT INDEX movieIndex FOR (m:Movie) ON EACH [m.description];

// G) Write a full text search query and search for some sample text of your choice.

CALL db.index.fulltext.queryNodes('movieIndex', 'the') YIELD node
RETURN node.title, node.description;
