import duckdb

# Connect to DuckDB
con = duckdb.connect("movie_graph.duckdb")

# -------------------------------
# Q2: Actors that directed a movie they also acted in
print("\nQ2: Actors that directed a movie they also acted in")
print(con.execute("""
SELECT DISTINCT p.name, m.title
FROM person p
JOIN acted_in ai ON p.person_id = ai.person_id
JOIN directed d ON p.person_id = d.person_id AND ai.movie_id = d.movie_id
JOIN movie m ON ai.movie_id = m.movie_id;
""").fetchall())

# -------------------------------
# Q3: Actor who played DeDe in Joe Versus the Volcano
print("\nQ3: Actor who played DeDe in Joe Versus the Volcano")
print(con.execute("""
SELECT p.name
FROM person p
JOIN acted_in ai ON p.person_id = ai.person_id
JOIN movie m ON ai.movie_id = m.movie_id
WHERE m.title = 'Joe Versus the Volcano'
  AND ai.roles LIKE '%DeDe%';
""").fetchall())

# -------------------------------
# Q4: Directors of movies where Madonna and Tom Hanks co-acted
print("\nQ4: Directors of movies where Madonna and Tom Hanks co-acted")
print(con.execute("""
SELECT DISTINCT p.name AS director_name, m.title
FROM movie m
JOIN acted_in ai1 ON m.movie_id = ai1.movie_id
JOIN person madonna ON ai1.person_id = madonna.person_id AND madonna.name = 'Madonna'
JOIN acted_in ai2 ON m.movie_id = ai2.movie_id
JOIN person tom ON ai2.person_id = tom.person_id AND tom.name = 'Tom Hanks'
JOIN directed d ON m.movie_id = d.movie_id
JOIN person p ON d.person_id = p.person_id;
""").fetchall())

# -------------------------------
# Q5: Actors in movies after 2009
print("\nQ5: Actors in movies after 2009")
print(con.execute("""
SELECT m.title, LISTAGG(p.name, ', ') AS cast
FROM movie m
JOIN acted_in ai ON m.movie_id = ai.movie_id
JOIN person p ON ai.person_id = p.person_id
WHERE m.released >= 2009
GROUP BY m.title;
""").fetchall())

# -------------------------------
# Q6: Other movies with same cast
print("\nQ6: Other movies with same cast")
print(con.execute("""
WITH movie_cast AS (
    SELECT m.movie_id, m.title AS movie_title,
           COUNT(DISTINCT p.person_id) AS cast_size
    FROM movie m
    JOIN acted_in ai ON m.movie_id = ai.movie_id
    JOIN person p ON ai.person_id = p.person_id
    WHERE TRY_CAST(m.released AS INTEGER) >= 2009
    GROUP BY m.movie_id, m.title
)
SELECT mc.movie_title AS original_movie,
       m2.title       AS other_movie
FROM movie_cast mc
JOIN movie m2 ON mc.movie_id <> m2.movie_id
WHERE NOT EXISTS (
    SELECT 1
    FROM acted_in ai
    JOIN person p ON ai.person_id = p.person_id
    WHERE ai.movie_id = mc.movie_id
      AND p.person_id NOT IN (
          SELECT ai2.person_id
          FROM acted_in ai2
          WHERE ai2.movie_id = m2.movie_id
      )
);
""").fetchall())

# -------------------------------
# Q8: Elder network (actors born before 1954 reachable from Clint Eastwood)
print("\nQ8: Elder network from Clint Eastwood")
print(con.execute("""
SELECT DISTINCT p2.name
FROM person clint
JOIN acted_in ai1 ON clint.person_id = ai1.person_id
JOIN movie m1 ON ai1.movie_id = m1.movie_id
JOIN acted_in ai2 ON m1.movie_id = ai2.movie_id
JOIN person p2 ON ai2.person_id = p2.person_id
WHERE clint.name = 'Clint Eastwood'
  AND p2.born IS NOT NULL
  AND p2.born <> 'null'
  AND CAST(p2.born AS INTEGER) < 1954
  AND p2.person_id <> clint.person_id;
""").fetchall())

# -------------------------------
# Q9: Adjacency list ordered by degree
print("\nQ9: Adjacency list ordered by degree")
print(con.execute("""
SELECT p.person_id, p.name,
       COUNT(ai.movie_id) AS degree,
       LISTAGG(ai.movie_id, ', ') AS adjacency
FROM person p
LEFT JOIN acted_in ai ON p.person_id = ai.person_id
GROUP BY p.person_id, p.name
ORDER BY degree DESC;
""").fetchall())

# -------------------------------
# Q10: Katz centrality approximation 
print("\nQ10: Katz centrality approximation")
print(con.execute("""
WITH RECURSIVE walks(actor_id, target_id, length, path) AS (
    -- Base case: 1-hop walks
    SELECT ai1.person_id AS actor_id,
           ai2.person_id AS target_id,
           1 AS length,
           ARRAY[ai1.person_id, ai2.person_id] AS path
    FROM acted_in ai1
    JOIN acted_in ai2 ON ai1.movie_id = ai2.movie_id
    WHERE ai1.person_id <> ai2.person_id

    UNION ALL

    -- Recursive case: extend walks by one hop, avoid cycles
    SELECT w.actor_id,
           ai2.person_id AS target_id,
           w.length + 1 AS length,
           w.path || ARRAY[ai2.person_id]
    FROM walks w
    JOIN acted_in ai1 ON w.target_id = ai1.person_id
    JOIN acted_in ai2 ON ai1.movie_id = ai2.movie_id
    WHERE w.length < 4
      AND ai2.person_id <> w.actor_id
      AND NOT ai2.person_id = ANY(w.path)
)

-- Final aggregation: deduplicate and apply penalties
SELECT p.name AS Actor,
       SUM(1.0 / length) AS KatzCentrality
FROM (
    SELECT DISTINCT actor_id, target_id, length
    FROM walks
) dedup
JOIN person p ON dedup.actor_id = p.person_id
GROUP BY p.name
ORDER BY KatzCentrality DESC
LIMIT 10;
""").fetchall())