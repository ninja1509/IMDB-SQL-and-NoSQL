
import json
from pandas import DataFrame
import string


#change structure of our dataset


    
#directors and movies directors 
    
    
import pandas as pd
direc=pd.read_csv('/Users/alfonsodamelio/dumps/directors.csv',delimiter=';',encoding='latin-1')
direc=direc.set_index('id')
direc=direc.to_dict('index')




movies_direc=pd.read_csv('/Users/alfonsodamelio/dumps/movies_directors.csv',delimiter=';')
movies_direc=movies_direc.set_index('director_id')
movies_direc=movies_direc.to_dict('index')


for key in direc.keys():
    if key in movies_direc.keys():
        direc[key]['movies_id']=movies_direc[key]['movie_id']
        direc[key]['director_id']=key
        
        
  
director_genres=pd.read_csv('/Users/alfonsodamelio/dumps/directors_genres.csv',delimiter=';')
director_genres=director_genres.set_index('director_id')
director_genres=director_genres.to_dict('index')      


for key in direc.keys():
    if key in director_genres.keys():
        direc[key]['director_genre']=director_genres[key]['genre']
        direc[key]['director_id']=key
        


        

dataframe=pd.DataFrame.from_dict(direc,orient='index')
dataframe=dataframe.set_index('movies_id')
dataframe=dataframe.to_dict('index') 



        


movies=pd.read_csv('/Users/alfonsodamelio/dumps/movies.csv',delimiter=';')
movies=movies.set_index('id')
movies=movies.to_dict('index')




for key in dataframe.keys():
    if key in movies.keys():
        dataframe[key]['movie_name']=movies[key]['name']
        dataframe[key]['movie_rank']=movies[key]['rank']
        dataframe[key]['movie_year']=movies[key]['year']
        dataframe[key]['movie_id']=key

        


movies_genre=pd.read_csv('/Users/alfonsodamelio/dumps/movies_genres.csv',delimiter=';')
movies_genre=movies_genre.set_index('movie_id')
movies_genre=movies_genre.to_dict('index')




for key in dataframe.keys():
    if key in movies_genre.keys():
        dataframe[key]['movie_genre']=movies_genre[key]['genre']
        dataframe[key]['movie_id']=key






roles=pd.read_csv('/Users/alfonsodamelio/dumps/roles.csv',delimiter=';')
roles=roles.set_index('movie_id')
roles=roles.to_dict('index')




for key in dataframe.keys():
    if key in roles.keys():
        dataframe[key]['actor_role']=roles[key]['role']
        dataframe[key]['actor_id']=roles[key]['actor_id']
        dataframe[key]['movie_id']=key




new=pd.DataFrame.from_dict(dataframe,orient='index')
new=new.set_index('actor_id')
new=new.to_dict('index') 



actors=pd.read_csv('/Users/alfonsodamelio/dumps/actors.csv',delimiter=';')
actors=actors.set_index('id')
actors=actors.to_dict('index')



for key in new.keys():
    if key in actors.keys():
        new[key]['actor_name']=actors[key]['first_name']
        new[key]['actor_last_name']=actors[key]['last_name']
        new[key]['actor_gender']=actors[key]['gender']
        new[key]['actor_id']=key


IMDB=[i for i in new.values()]




#write json to work in local
with open('/Users/alfonsodamelio/Desktop/DATA SCIENCE/2°SEMESTRE/Data Management(Rosati)/HW3/IMDB.json', 'w') as fp:
    json.dump(IMDB, fp)


#read json
with open('/Users/alfonsodamelio/Desktop/DATA SCIENCE/2°SEMESTRE/Data Management(Rosati)/HW3/IMDB.json',encoding="utf-8") as json_data:
    imdb = json.load(json_data)
    


#read json of other dataset
with open('/Users/alfonsodamelio/Desktop/DATA SCIENCE/2°SEMESTRE/Data Management(Rosati)/HW3/movie.json',encoding="utf-8") as json_data:
    movie_json = json.load(json_data)
    
    
    
    
for i in movie_json:

    if len(i['Actors'].split())>=2:
        lista=i['Actors'].split()[0:2]
        i['actor_name']=lista[0]
        i['actor_last_name']="".join((char for char in lista[1] if char not in string.punctuation))
        del i['Actors']
            
    else:
        pass
    
for i in movie_json:  
    
    if len(i['Director'].split())>=2:
        lista2=i['Director'].split()[0:2]
        i['director_name']=lista2[0]
        i['director_last_name']=lista2[1]
        del i['Director']
    else:
        pass
    
for i in movie_json:  
    
    if len(i['genre'].split(','))>=2:
        lista3=i['genre'].split(',')[0:2]
        i['movie_genre']=lista3[0]
        i["actor_gender"]="NaN"
        i["actor_id"]="NaN"
        i["director_id"]="NaN"
        i["actor_role"]="NaN"
        
        del i['genre']
    else:
        pass
    
    




    

### remove keys we don't need

def removekey(d, key):
    r = dict(d)
    del r[key]
    return r

nuovo_movies=[]
for i in movie_json:
    nuovo_movies.append(removekey(i, "Description"))

nuovo_movies1=[]
for i in nuovo_movies:
    nuovo_movies1.append(removekey(i, "Runtime (Minutes)"))
del nuovo_movies

nuovo_movies2=[]
for i in nuovo_movies1:
    nuovo_movies2.append(removekey(i, "Rank"))
del nuovo_movies1

nuovo_movies3=[]
for i in nuovo_movies2:
    nuovo_movies3.append(removekey(i, "Votes"))
del nuovo_movies2

nuovo_movies4=[]
for i in nuovo_movies3:
    nuovo_movies4.append(removekey(i, "Revenue (Millions)"))
del nuovo_movies3

imdb2=[]
for i in nuovo_movies4:
    imdb2.append(removekey(i, "Metascore"))
del nuovo_movies4







##############################  Querying  #############################




#connect to the client DB with pymongo 
import pymongo
from pymongo import MongoClient
uri='mongodb://dma:alfo11295@ds123490.mlab.com:23490/movies_1625'
client=MongoClient(uri)
db=MongoClient(uri).get_database('movies_1625')
db.authenticate('dma','alfo11295')
coll=db.imdb

#inserting data on MLAB
#coll.insert_many(imdb)
#coll.insert_many(imdb2)


#1 return the names and year of the movies where Kevin Space starred in
k_s=coll.find({
    "actor_name": 'Kevin',
    "actor_last_name": 'Spacey'
},projection={"actor_name":1,"actor_last_name":1,"movie_rank":1,"movie_year":1,"movie_name":1,"_id":0})

print()    
print (" Actor name | Actor last name |  Movie year   |       Movie name     |  Rank rate ")
print ("----------------------------------------------------------------------------------")
for i in k_s:
    print(" %10s |   %10s    |      %.f     | %20s |    %.1f" % (i['actor_name'],i['actor_last_name'],i["movie_year"],i["movie_name"],round(i['movie_rank'],2)))
    
      
#2 return name,rank,main actor_name and last name of the movies directed by Quentin Tarantino whom have more than 6 as rank rate
# order by rank in descending order 
result=coll.find({
   "movie_rank":{ "$gte":6},
   "director_name":"Quentin",
   "director_last_name":"Tarantino"
   
}, projection={"actor_name":1,"actor_last_name":1,"movie_rank":1,"movie_name":1,"_id":0}).sort([("movie_rank", pymongo.ASCENDING)])
print()    
print (" Actor name | Actor last name |      Movie name      |  Rank rate ")
print ("------------------------------------------------------------------")
for i in result:
    print(" %10s | %10s      | %20s |    %.1f" % (i['actor_name'],i['actor_last_name'],i["movie_name"],round(i['movie_rank'],2)))
    

#3 count all the films of 2016 and return mean of rank 
film_2016=coll.aggregate([{"$match":{"movie_year":2016}},
    {"$group":{"_id":"$movie_year","avg":{"$avg":"$movie_rank"},"count":{"$sum":1}}}])
print()
print (" Movie year |  Average rank   | Number of films")
print ("-------------------------------------------------")
for i in film_2016:
    print(" %10s |       %.1f      |    %.f" % (i['_id'],i["avg"],i["count"]))
    




#4 Mean of the rank of the film directed by Steven Spielberg where movie genre is Action
mean_stev=coll.aggregate([{"$match":{"director_name": 'Steven',
    "director_last_name": 'Spielberg',"movie_genre":"Action"}}, {"$group":{"_id":"$movie_genre","avg":{"$avg":"$movie_rank"}}}])
print()
print (" Movie Genre | Average rank")
print ("---------------------------")
for i in mean_stev:
    print(" %7s     |   %.1f" % (i['_id'],i["avg"]))
    


    
#5 mean of the rank of the film group by gender
mean_gender=coll.aggregate([{"$match":{"$or":[{"actor_gender":"M"},{"actor_gender":"F"}]}}, {"$group":{"_id":"$actor_gender","avg":{"$avg":"$movie_rank"}}}])
print()
print ("Actor Gender | Average rank")
print ("--------------------------")
for i in mean_gender:
    print("  %7s    |   %.1f" % (i['_id'],i["avg"]))
   
    

#6 number of films where main actor is Female or male
count_film_bygender=coll.aggregate([{"$match":{"$or":[{"actor_gender":"M"},{"actor_gender":"F"}]}}, {"$group":{"_id":"$actor_gender","count":{"$sum":1}}}])
print()
print ("Actor Gender | Number of films")
print ("-----------------------------")
for i in count_film_bygender:
    print(" %7s     |   %.f" % (i['_id'],round(i["count"])))
    



#7 return last name of the directors whose film have rank greater than 8
direct_result=coll.find({
   "movie_rank":{ "$gte":8},  
}, projection={"director_last_name":1,"movie_rank":1,"movie_name":1,"_id":0}).sort([("movie_rank", pymongo.DESCENDING)]).limit(10)
print()
print (" Director last name |      Movie name      |   Rank rate ")
print ("---------------------------------------------------------")
for i in direct_result:
    print(" %18s | %20s |    %.1f" % (i['director_last_name'],i["movie_name"],round(i['movie_rank'],2)))
  
    

#8 return the name,last name of the directors of the films that reached the maximum rank in 2006
max_rank_2006=coll.find_one({"movie_year":2006,"movie_rank": {"$exists": True}}, projection={"director_name":1,"director_last_name":1,"movie_genre":1,"movie_rank":1,"movie_name":1,"_id":0}, sort=[("movie_rank", -1)])
print()
print (" Director name | Director last name | Movie genre |          Movie name         | Movie rank")
print ("--------------------------------------------------------------------------------------------")
print(" %11s   |  %12s      |   %7s   |     %s     |   %.1f" % (max_rank_2006['director_name'],max_rank_2006['director_last_name'],max_rank_2006["movie_genre"],max_rank_2006["movie_name"],max_rank_2006["movie_rank"]))
    
    
  
