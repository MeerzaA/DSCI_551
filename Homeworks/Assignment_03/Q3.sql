USE CINEMA;

-- 1) Find titles of the longest movies. Note that there might be more than such movie.
select
    title,
    length
from
    Movies
where length=(select max(length) from Movies);

-- 2) Find out titles of movies that contain "Twilight" and are directed by "Steven Spielberg".
select 
    Movies.title 
from 
    Movies 
join 
    DirectedBy on Movies.id = DirectedBy.movie_id 
join 
    Directors on DirectedBy.director_id = Directors.id 
where 
    Directors.name = 'Steven Spielberg' and Movies.title like '%Twilight%';

-- 3) Find out how many movies "Tom Hanks" has acted in.
select 
    count(*) 
from 
    ActIn 
join 
    Actors on ActIn.actor_id = Actors.id 
where 
    Actors.name = 'Tom Hanks';


-- 4) Find out which director directed only a single movie.
select 
    Directors.name
from 
    Directors
join 
    DirectedBy on Directors.id = DirectedBy.director_id
group by 
    Directors.name 
having 
    COUNT(DirectedBy.movie_id) = 1;

-- 5) Find titles of movies which have the largest number of actors. Note that there may be multiple such movies.
select 
    Movies.title, COUNT(ActIn.actor_id) as total
from 
    Movies
join 
    ActIn on Movies.id = ActIn.movie_id
group by 
    Movies.title 
having
    COUNT(ActIn.actor_id) = (
    select 
        MAX(total)
    from (
        select 
            COUNT(actor_id) as total
        from 
            ActIn
        group by 
            movie_id
    ) as Most_Actors );


-- 6) Find names of actors who played in both English (language = "en") and French ("fr") movies.
select 
    Actors.name
from 
    Actors
join 
    ActIn on Actors.id = ActIn.actor_id
join 
    Movies on ActIn.movie_id = Movies.id
Where 
    Movies.language in ('en','fr') 
group by  
   Actors.name
having 
    COUNT(Distinct Movies.language) = 2;

-- 7) Find names of directors who only directed English movies.
SELECT Directors.name
FROM Directors 
WHERE NOT EXISTS (
    SELECT 1
    FROM Movies 
    WHERE Movies.language <> 'en'
    AND Movies.id IN (
        SELECT DirectedBy.movie_id
        FROM DirectedBy 
        WHERE DirectedBy.director_id = Directors.id
    )
);
    
  

