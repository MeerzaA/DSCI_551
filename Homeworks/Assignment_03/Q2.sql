-- Use the CINEMA database
USE CINEMA;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS DirectedBy, ActIn, Directors, Actors, Movies;


-- Please create the tables as per the structure given.
-- Remember to consider appropriate data types and primary/foreign key constraints.

-- Movies(id, title, year, length, language)
CREATE TABLE Movies (      
id        INT,    
title     VARCHAR(50),      
year      INT,      
length    INT,      
language  VARCHAR(50),         
PRIMARY KEY (id));

-- Actors(id, name, gender)
CREATE TABLE Actors (      
id        INT,    
name      VARCHAR(50),      
gender    VARCHAR(25),               
PRIMARY KEY (id));

-- ActIn(actor_id, movie_id)
CREATE TABLE ActIn (      
actor_id  INT,    
movie_id  INT,      
PRIMARY KEY (actor_id,movie_id),
FOREIGN KEY (actor_id) REFERENCES Actors(id), 
FOREIGN KEY (movie_id) REFERENCES Movies(id)); 

-- Directors(id, name, nationality)
CREATE TABLE Directors (      
id          INT,    
name        VARCHAR(50),      
nationality VARCHAR(50),          
PRIMARY KEY (id));

-- DirectedBy(movie_id, director_id)
CREATE TABLE DirectedBy (      
director_id INT,    
movie_id    INT,      
PRIMARY KEY (director_id,movie_id),
FOREIGN KEY (director_id) REFERENCES Directors(id), 
FOREIGN KEY (movie_id) REFERENCES Movies(id)); 


-- Please insert sample data into the tables created above.

-- Movies 
INSERT INTO Movies   
	VALUES('0001', 'The God Father', 1972, 175, 'en');

INSERT INTO Movies   
	VALUES('0002', 'The Shawshank Redemption', 1994, 182, 'en');

INSERT INTO Movies   
	VALUES('0003', 'Schindler List', 1993, 195, 'en');

INSERT INTO Movies   
	VALUES('0004', 'Twilight', 2008, 122, 'en');

INSERT INTO Movies   
	VALUES('0005', 'Twilight List', 2025, 200, 'en');

INSERT INTO Movies   
	VALUES('0006', 'Twilight List', 2025, 200, 'fr');


-- Actors 
INSERT INTO Actors   
	VALUES('01', 'Marlone Brando', 'm');

INSERT INTO Actors   
	VALUES('02', 'Tim Robbins', 'm');

INSERT INTO Actors   
	VALUES('03', 'Liam Neeson', 'm');

INSERT INTO Actors   
	VALUES('04', 'Kristen Stewert', 'f');

INSERT INTO Actors   
	VALUES('05', 'Liam Stewert', 'f');

-- ActIn
INSERT INTO ActIn   
	VALUES('01', '0001');

INSERT INTO ActIn   
	VALUES('02', '0002');

INSERT INTO ActIn   
	VALUES('03', '0003');

INSERT INTO ActIn   
	VALUES('04', '0004');

INSERT INTO ActIn   
	VALUES('03', '0004');

INSERT INTO ActIn   
	VALUES('02', '0004');

INSERT INTO ActIn   
	VALUES('05', '0004');

INSERT INTO ActIn   
	VALUES('05', '0005');

INSERT INTO ActIn   
	VALUES('01', '0005');

INSERT INTO ActIn   
	VALUES('02', '0005');

INSERT INTO ActIn   
	VALUES('03', '0005');

INSERT INTO ActIn   
	VALUES('01', '0006');

INSERT INTO ActIn   
	VALUES('02', '0006');


-- Directors
INSERT INTO Directors   
	VALUES('001', 'Francis Ford Coppola','italian');

INSERT INTO Directors   
	VALUES('002', 'Frank Darabont','english');

INSERT INTO Directors   
	VALUES('003', 'Steven Spielberg','french');

INSERT INTO Directors   
	VALUES('004', 'Catherine Hardwicke','english');

-- DirectedBy
INSERT INTO DirectedBy   
	VALUES('001', '0001');

INSERT INTO DirectedBy   
	VALUES('002', '0002');

INSERT INTO DirectedBy   
	VALUES('003', '0003');

INSERT INTO DirectedBy   
	VALUES('004', '0004');

INSERT INTO DirectedBy   
	VALUES('003', '0005');

INSERT INTO DirectedBy   
	VALUES('004', '0006');

-- Note: Testing will be conducted on a blind test set, so ensure your table creation and data insertion scripts are accurate and comprehensive.
