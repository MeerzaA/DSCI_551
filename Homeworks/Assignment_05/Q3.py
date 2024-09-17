from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Film Dataset Analysis").getOrCreate()

# Load the datasets as DataFrames
film_df = spark.read.csv('film.csv', header=True, inferSchema=True)
actor_df = spark.read.csv('actor.csv', header=True, inferSchema=True)
film_actor_df = spark.read.csv('film_actor.csv', header=True, inferSchema=True)

# Convert DataFrames to RDDs
film_rdd = film_df.rdd
actor_rdd = actor_df.rdd
film_actor_rdd = film_actor_df.rdd

def query_a(film_rdd):
    """
    Select title and description from the film dataset where the rating is 'PG'.

    SELECT title, description
    FROM film
    WHERE rating = "PG"
    LIMIT 5

    """
    # Implement the RDD transformation and action here
    film_rdd = film_rdd.filter(lambda r: r['rating']=="PG").map(lambda r: (r['title'], r['description']))

    return film_rdd

"""
# %query_a output:
[('ACADEMY DINOSAUR', 'A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies'), 
('AGENT TRUMAN', 'A Intrepid Panorama of a Robot And a Boy who must Escape a Sumo Wrestler in Ancient China'), 
('ALASKA PHANTOM', 'A Fanciful Saga of a Hunter And a Pastry Chef who must Vanquish a Boy in Australia'), 
('ALI FOREVER', 'A Action-Packed Drama of a Dentist And a Crocodile who must Battle a Feminist in The Canadian Rockies'), 
('AMADEUS HOLY', 'A Emotional Display of a Pioneer And a Technical Writer who must Battle a Man in A Baloon')]
"""

def query_b(film_rdd):
    """
    Select the average replacement cost grouped by rating for films longer than 60 minutes, having at least 160 films per rating.

    SELECT rating, avg(replacement_cost)
    FROM film
    WHERE length >= 60
    GROUP BY rating
    HAVING count(*) >= 160

    """
    # Implement the RDD transformation and action here
    film_rdd = film_rdd.filter(lambda r:r['length']>=60)\
    .map(lambda r: (r['rating'], (r['replacement_cost'], 1)))\
    .aggregateByKey((0.0,0),\
        lambda U, x: (U[0]+x[0], U[1]+x[1]),\
        lambda U, V: (U[0]+V[0], U[1]+V[1]))\
    .filter(lambda kv: kv[1][1]>= 160)\
    .mapValues(lambda x: x[0] / x[1])

    return film_rdd

"""
# %query_b output:
[('PG', 18.84465116279062), 
('PG-13', 20.579108910890984), 
('NC-17', 20.265132275132174), 
('R', 20.294347826086863)]
"""

def query_c(film_actor_rdd):
    """
    Select actor IDs that appear in both film ID 1 and film ID 23.

    SELECT actor_id FROM film_actor WHERE film_id = 1)
    intersect
    (SELECT actor_id FROM film_actor where film_id = 23)

    """
    # Implement the RDD transformation and action here
    film_actor_1 = film_actor_rdd.filter(lambda r: r['film_id'] == 1).map(lambda r:(r['actor_id']))
    film_actor_2 = film_actor_rdd.filter(lambda r: r['film_id'] == 23).map(lambda r:(r['actor_id']))

    return film_actor_1.intersection(film_actor_2)
"""
# %query_c output:
[1]
"""

def query_d(actor_rdd, film_actor_rdd):
    """
    Select distinct first name and last name of actors who acted in films 1, 2, or 3.

    SELECT DISTINCT first_name, last_name
    FROM actor JOIN film_actor ON actor.actor_id = film_actor.actor_id
    WHERE film_id in (1, 2, 3)
    ORDER BY first_name\
    LIMIT 5

    """
    # Implement the RDD transformation and action here
    actorlist = film_actor_rdd.filter(lambda x: x["film_id"] in [1,2,3]).map(lambda x: x["actor_id"]).collect()

    distinctlist = actor_rdd.filter(lambda x: x["actor_id"]in actorlist).distinct().map(
        lambda x: (x["first_name"], x["last_name"])).map(lambda x: (x[0], x[1])).sortByKey()
    
    return distinctlist

"""
# %query_d output:
[('BOB', 'FAWCETT'), 
('CAMERON', 'STREEP'), 
('CHRIS', 'DEPP'), 
('CHRISTIAN', 'GABLE'), 
('JOHNNY', 'CAGE')]
"""

def query_e(film_rdd):
    """
    Select rental duration, rating, minimum, maximum, and average length, and count of films, grouped by rental duration and rating, ordered by rental duration descending.

    SELECT rental_duration, rating, min(length), max(length), avg(length), count(length)
    FROM film
    GROUP BY rental_duration, rating
    ORDER BY rental_duration desc
    LIMIT 10

    """
    # Implement the RDD transformation and action here
    filmlist = film_rdd.map(lambda x: ((x["rental_duration"], x["rating"]), (x["length"], 1)))
    sortedfilmlist = filmlist.reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[0], b[0]), a[0] + b[0], a[1] + b[1]))
    final_list = sortedfilmlist.map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1], x[1][2] / x[1][3], x[1][3]))

    return final_list.sortByKey(ascending=False)

"""
# %query_e output:
[(7, 'NC-17', 48, 55, 1.9433962264150944, 53), 
 (7, 'PG-13', 48, 107, 0.8857142857142857, 175), 
 (7, 'PG', 46, 105, 1.755813953488372, 86), 
 (7, 'G', 49, 80, 1.7432432432432432, 74), 
 (7, 'R', 59, 139, 1.2222222222222223, 162), 
 (6, 'PG', 49, 61, 0.6432748538011696, 171), 
 (6, 'G', 57, 183, 3.0, 80), 
 (6, 'PG-13', 46, 100, 0.8156424581005587, 179), 
 (6, 'R', 54, 111, 1.1956521739130435, 138), 
 (6, 'NC-17', 48, 105, 1.7790697674418605, 86)]
"""


def main():

    print("Query A:")
    print(query_a(film_rdd).take(5))
    
    print("Query B:")
    print(query_b(film_rdd).collect())

    print("Query C:")
    print(query_c(film_actor_rdd).collect())

    print("Query D:")
    print(query_d(actor_rdd, film_actor_rdd).take(5))

    print("Query E:")
    print(query_e(film_rdd).take(10))

if __name__ == "__main__":
    main()
