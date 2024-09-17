from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

# Initialize SparkSession and load the datasets
spark = SparkSession.builder.appName("Film Dataset Analysis").getOrCreate()
film = spark.read.csv('film.csv', header=True, inferSchema=True)
actor = spark.read.csv('actor.csv', header=True, inferSchema=True)
film_actor = spark.read.csv('film_actor.csv', header=True, inferSchema=True)

def query_a(film_df):
    """
    Question: Select title and description from the film dataset where the rating is 'PG'.

    SELECT title, description
    FROM film
    WHERE rating = "PG"
    LIMIT 5

    """
    # Implement the query here
    film_df = film_df.filter(film_df.rating == "PG").select('title', 'description').limit(5)

    return film_df

"""
query_a Output:
+----------------+--------------------+
|           title|         description|
+----------------+--------------------+
|ACADEMY DINOSAUR|A Epic Drama of a...|
|    AGENT TRUMAN|A Intrepid Panora...|
|  ALASKA PHANTOM|A Fanciful Saga o...|
|     ALI FOREVER|A Action-Packed D...|
|    AMADEUS HOLY|A Emotional Displ...|
+----------------+--------------------+
"""

def query_b(film_df):
    """
    Question: Select the average replacement cost grouped by rating for films longer than 60 minutes, having at least 160 films per rating.

    SELECT rating, avg(replacement_cost)
    FROM film
    WHERE length >= 60
    GROUP BY rating
    HAVING count(*) >= 160

    """
    # Implement the query here
    film_df = film_df.filter(film_df.length >= 60).groupBy('rating').agg(fc.avg('replacement_cost').alias('average'), fc.count('*').alias('cnt')).filter(
        'cnt>=160').select('rating', 'average')

    return film_df

"""
query_b Output:
+------+------------------+
|rating|           average|
+------+------------------+
|    PG| 18.84465116279062|
| NC-17|20.265132275132174|
|     R|20.294347826086863|
| PG-13|20.579108910890984|
+------+------------------+
"""

def query_c(film_actor_df):
    """
    Question: Select actor IDs that appear in both film ID 1 and film ID 23.

    SELECT actor_id 
    FROM film_actor 
    WHERE film_id = 1)
     intersect
    (SELECT actor_id FROM film_actor where film_id = 23)

    """
    # Implement the query here
    film_actor_df = film_actor_df.filter(film_actor_df.film_id == 1).select('actor_id').intersect(film_actor_df.filter(film_actor_df.film_id == 23).select('actor_id'))

    return film_actor_df

"""
query_c Output:
+--------+
|actor_id|
+--------+
|       1|
+--------+
"""

def query_d(actor_df, film_actor_df):
    """
    Question: Select distinct first name and last name of actors who acted in films 1, 2, or 3. Order the result by first name.

    SELECT DISTINCT first_name, last_name
    FROM actor JOIN film_actor ON actor.actor_id = film_actor.actor_id
    WHERE film_id in (1, 2, 3)
    ORDERBY first_name
    LIMIT 5

    """
    # Implement the query here
    #film_actor_df = film_actor_df.filter(film_actor_df.film_id.isin([1,2,3]))
    #actor_df = actor_df.join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id).select('first_name', 'last_name').distinct().orderBy('first_name').limit(5)

    return actor_df.join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id).filter(
        film_actor_df.film_id.isin([1,2,3])).select('first_name', 'last_name').distinct().orderBy('first_name').limit(5)

"""
query_d Output:
+----------+---------+
|first_name|last_name|
+----------+---------+
|       BOB|  FAWCETT|
|   CAMERON|   STREEP|
|     CHRIS|     DEPP|
| CHRISTIAN|    GABLE|
|    JOHNNY|     CAGE|
+----------+---------+
"""

def query_e(film_df):
    """
    Question: Select rental duration, rating, minimum, maximum, and average length, and count of films, grouped by rental duration and rating, ordered by rental duration descending.

    SELECT rental_duration, rating, min(length), max(length), avg(length), count(length)
    FROM film
    GROUP BY rental_duration, rating
    ORDER BY rental_duration desc
    LIMIT 10

    """
    # Implement the query here
    film_df = film_df.groupBy('rental_duration', 'rating').agg(
        fc.min('length').alias('Min'), fc.max('length').alias('Max'), fc.avg('length').alias('Average'), fc.count('length').alias('Count')).orderBy(
        'rental_duration', ascending=False).limit(10)

    return film_df

"""
query_e Output:
+---------------+------+---+---+------------------+-----+
|rental_duration|rating|Min|Max|           Average|Count|
+---------------+------+---+---+------------------+-----+
|              7|     R| 59|185|131.27272727272728|   33|
|              7| NC-17| 48|179|             118.7|   40|
|              7|    PG| 46|182|111.95555555555555|   45|
|              7|     G| 49|185|116.34482758620689|   29|
|              7| PG-13| 48|185|118.27272727272727|   44|
|              6| NC-17| 48|184|111.78947368421052|   57|
|              6| PG-13| 46|185|            118.52|   50|
|              6|     G| 57|183|             128.0|   39|
|              6|     R| 54|181|127.18518518518519|   27|
|              6|    PG| 49|182|104.82051282051282|   39|
+---------------+------+---+---+------------------+-----+
"""

def main():
    print("Query A:")
    query_a(film).show()

    print("Query B:")
    query_b(film).show()

    print("Query C:")
    query_c(film_actor).show()

    print("Query D:")
    query_d(actor, film_actor).show()

    print("Query E:")
    query_e(film).show()

if __name__ == "__main__":
    main()
