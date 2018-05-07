CREATE SCHEMA `IMDB` ;
use `IMDB`;


CREATE TABLE `IMDB`.`actors` (
  `id` INT(11) NOT NULL DEFAULT '0',
  `first_name` VARCHAR(100) NULL DEFAULT NULL,
  `last_name` VARCHAR(100) NULL DEFAULT NULL,
  `gender` CHAR(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `actors_first_name` (`first_name` ASC),
  INDEX `actors_last_name` (`last_name` ASC));

LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/actors.csv' 
INTO TABLE actors 
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE `IMDB`.`directors` (
  `id` INT(11) NOT NULL DEFAULT '0',
  `first_name` VARCHAR(100) NULL DEFAULT NULL,
  `last_name` VARCHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `directors_first_name` (`first_name` ASC),
  INDEX `directors_last_name` (`last_name` ASC));

LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/directors.csv' 
INTO TABLE directors 
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE `IMDB`.`directors_genres` (
  `director_id` INT(11) NOT NULL,
  `genre` VARCHAR(100) NOT NULL,
  `prob` FLOAT NULL DEFAULT NULL,
  PRIMARY KEY (`director_id`, `genre`),
  INDEX `directors_genres_director_id` (`director_id` ASC),
  CONSTRAINT `directors_genres_ibfk_1`
    FOREIGN KEY (`director_id`)
    REFERENCES `IMDB`.`directors` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE);
    
LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/directors_genres.csv' 
INTO TABLE directors_genres
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE `IMDB`.`movies` (
  `id` INT(11) NOT NULL DEFAULT '0',
  `name` VARCHAR(100) NULL DEFAULT NULL,
  `year` INT(11) NULL DEFAULT NULL,
  `rank` FLOAT NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  INDEX `movies_name` (`name` ASC));
  
LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/movies.csv' 
INTO TABLE movies
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE `IMDB`.`movies_directors` (
  `director_id` INT(11) NOT NULL,
  `movie_id` INT(11) NOT NULL,
  PRIMARY KEY (`director_id`, `movie_id`),
  INDEX `movies_directors_director_id` (`director_id` ASC),
  INDEX `movies_directors_movie_id` (`movie_id` ASC),
  CONSTRAINT `movies_directors_ibfk_1`
    FOREIGN KEY (`director_id`)
    REFERENCES `IMDB`.`directors` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `movies_directors_ibfk_2`
    FOREIGN KEY (`movie_id`)
    REFERENCES `IMDB`.`movies` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE);

LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/movies_directors.csv' 
INTO TABLE movies_directors
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE `IMDB`.`movies_genres` (
  `movie_id` INT(11) NOT NULL,
  `genre` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`movie_id`, `genre`),
  INDEX `movies_genres_movie_id` (`movie_id` ASC),
  CONSTRAINT `movies_genres_ibfk_1`
    FOREIGN KEY (`movie_id`)
    REFERENCES `IMDB`.`movies` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE);
    
LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/movies_genres.csv' 
INTO TABLE movies_genres
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE `IMDB`.`roles` (
  `actor_id` INT(11) NOT NULL,
  `movie_id` INT(11) NOT NULL,
  `role` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`actor_id`, `movie_id`, `role`),
  INDEX `actor_id` (`actor_id` ASC),
  INDEX `movie_id` (`movie_id` ASC),
  CONSTRAINT `roles_ibfk_1`
    FOREIGN KEY (`actor_id`)
    REFERENCES `IMDB`.`actors` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `roles_ibfk_2`
    FOREIGN KEY (`movie_id`)
    REFERENCES `IMDB`.`movies` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE);

    
LOAD DATA LOCAL INFILE '/Users/alfonsodamelio/dumps/roles.csv' 
INTO TABLE roles
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;




###################### QUERY ON DB

use IMDB;

## 1) Find the name,year and rank of the movies whose rank and year are respectively rank>9 and year>2000.
select name,year,rank
from movies
where rank>9 and year>2000
order by rank;

## 2) Find the name and surname of the director who has directed Batman.
select first_name,last_name
from movies join movies_directors on movies.id=movies_directors.movie_id 
			join directors on movies_directors.director_id=directors.id
where name='Batman';


## 3) Return the name and the year of the movies directed by Quentin Tarantino.
select movies.name, year
from movies join movies_directors on movie_id = movies.id 
		join directors on directors.id = director_id
where directors.first_name = "Quentin" and directors.last_name = "Tarantino"
order by year;


## 4) Return the name of the Jim Carrey movies where the rank >= 6.
select name, rank
from actors join roles on actors.id=roles.actor_id 
		join movies on roles.movie_id=movies.id
where first_name='Jim' and last_name='Carrey' and rank >= 6
order by rank desc;


## 4.2) 
select name,rank
from movies 
where rank>=6 and id in (select movie_id
		         from roles
                         where actor_id  in (select id 
					     from actors
					     where first_name='Jim' and last_name='Carrey'))
					     order by rank desc;

## 5) Find the movies name played by Samuel L. Jackson
select movies.name
from roles join actors on actor_id = id join movies on movie_id = movies.id
where first_name = "Samuel L." and last_name = "Jackson";


## 6) Return the rank of the movies directed by Quentin Tarantino where Samuel L. Jackson worked in
select name,year
from movies join movies_directors on  movie_id = movies.id
     join directors on directors.id=director_id
where directors.first_name = "Quentin" and directors.last_name = "Tarantino" and movies.id in (select movie_id
                                                                                                from roles
                                                                                                where actor_id in (select id
                                                                                                                    from actors
                                                                                                                    where first_name = "Samuel L." and last_name="Jackson"))
                                                                                                                    order by year;

## 7) Find the rank average grouped by genre
select genre,round(avg(rank),1) as average
from movies join movies_genres on id = movie_id
group by genre
order by average desc;


## 8) Find the total number of movies for each director
select first_name,last_name,count(*) as total_number
from directors join movies_directors on id = director_id
group by first_name,last_name
order by total_number desc;


## 9) Find the name of directors whose movies have rank>8. Ordered by year
select distinct first_name,last_name,name as movies,rank,year
from movies join movies_directors on director_id = id 
			join directors on director_id = directors.id
having rank >= all (select rank
                    from movies
                    having rank = 8)
order by rank,year;
        

                

#################### OPTIMIZE QUERY

use IMDB;


## 6) Return the rank of the movies directed by Quentin Tarantino where Samuel L. Jackson worked in 
##[OLD-time --------->9secs]
select name,year
from movies join movies_directors on  movie_id = movies.id
     join directors on directors.id=director_id
where directors.first_name = "Quentin" and directors.last_name = "Tarantino" and movies.id in  ( select movie_id
									                         from roles
                                                                                                 where actor_id in ( select id
                                                                                                                      from actors
                                                                                                                      where first_name = "Samuel L." and last_name="Jackson"))
                                                                                                                      order by year;




## 9) Find the name of directors whose movies have rank>8. Ordered by year 
##[OLD-time---->1.3 secs]
select distinct first_name,last_name,name as movies,rank,year
from movies join movies_directors on director_id = id 
			join directors on director_id = directors.id
having rank >= all (select rank
		    from movies
                    having rank = 8)
order by rank,year;
        
        

## 10) Find the rank average of the movies based on the actors gender 
##[OLD-time ----->20 secs]
select gender, round(avg(rank),1) as rank
from actors join roles on id = actor_id join movies on movies.id = roles.movie_id
group by gender;


                

##11) VIEW WHERE we select all the films for each actors with role himself --> autobiografia
##[OLD-time ---->11 secs]

create view himself_films as
select distinct count(movies.id) as n_of_movies,round(avg(rank),1) as rank_rate ,first_name as name,last_name as surname,actor_id as id
from movies join roles on movies.id=movie_id join actors on actor_id=actors.id
where role='Himself'
group by actor_id
order by n_of_movies desc;

##so take ones with maximum..
select  first_name as name ,last_name as surname,rank_rate,n_of_movies
from actors join himself_films on actors.id=himself_films.id
where n_of_movies>=all (select n_of_movies
			from himself_films);



## 12) nomi, cognome ,numero di film di tutti i direttori e media dei rank dei film diretti
##[OLD-time ---> out of time]
select distinct first_name,last_name,count(movies.id),round(avg(rank),1) as rank_average
from movies_genres join movies on movies_genres.movie_id=movies.id join movies_directors on  movies.id=movies_directors.movie_id 
     join directors on directors.id=director_id
group by director_id
order by count(movies.id) desc;

