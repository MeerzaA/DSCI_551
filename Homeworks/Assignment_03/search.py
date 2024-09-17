# IMPORT LIBRARIES
import sys
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd
pymysql.install_as_MySQLdb()

# NOTE: DO NOT modify any variable or function names given in the template!

# Choose EITHER SQLAlchemy OR PyMySQL:

# OPTION 1: SQLAlchemy
DATABASE_URI = 'mysql+mysqldb://dsci551:Dsci-551@localhost/CINEMA'
my_conn = sqlalchemy.create_engine(DATABASE_URI)

# OPTION 2: PyMySQL
# Replace the placeholders with the actual database connection details
# DATABASE_CONNECTION_PARAMS = {

# }


def get_movies_by_actor(actor_name):
    """
    Fetches titles of movies that the specified actor has acted in.
    INPUT: actor_name (str) - The name of the actor
    RETURN: A list of movie titles (list)
    
    Sample Terminal Command:python3 search.py "John Doe"
    Expected Sample Output: Movies featuring John Doe: ['The Great Adventure', 'Dreams of Space']
    """
    
    Movies = pd.read_sql(''' 
        select 
            Movies.title 
        from 
            Actors 
        join      
            ActIn on Actors.id = ActIn.actor_id 
        join 
            Movies on ActIn.movie_id = Movies.id 
        where 
            Actors.name = "{input}"  
        group by 
            Movies.title;'''.format(input=actor_name), my_conn)


    # Placeholder for the result list:
    movies = Movies['title'].values.tolist()

    ## Your code goes here

    return movies


# Use the below main method to test your code
if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Usage: python search.py '<actor_name>'")
        sys.exit(1)
    actor_name = sys.argv[1]
    movies = get_movies_by_actor(actor_name)

    if movies:
        print(f"Movies featuring {actor_name}: {movies}")
    else:
        print(f"No movies found for actor {actor_name}.")
