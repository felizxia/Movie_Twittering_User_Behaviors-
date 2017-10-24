import json
import re
import omdb
import csv
import pandas as pd
import requests

## data pre-processing

movies=pd.read_csv('movies.csv',sep=",")
#rexp_imdb = re.compile(r'[0-9]+', flags=re.IGNORECASE)
movies['movie_id']=movies['movie_id'].str.strip("'")
movies['movie_id']= 'tt' + movies['movie_id']


movies.drop(movies.columns[0],axis=1)
# release_date	imdb_ratings	country 	rating_account
imdb_id = movies.movie_id.tolist()
imdb_table=[]
imdb_j_spark= open('allmovies_imdb.txt','w')
to_do_id= open('undo_id.txt','w')
for id in imdb_id:
    id=id.strip()
    url = "http://www.omdbapi.com/?i=" + str(id) + "&apikey=2e376ccf"
    r = requests.get(url, auth=('user', 'pass'))
    try:
        jsons=r.text
        imdb_j_spark.write(jsons)
        imdb_j_spark.write('\n')
    except:
        print(str(id))
        to_do_id.write(str(id))
        to_do_id.write('\n')

## spark part

import sys
import json
import pyspark
from pyspark import SparkConf,SparkContext
sc = SparkContext(appName="Projects_felizxia")
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
sqlContext = SQLContext(sc)
from pyspark.sql import *
from operator import add
import time
import datetime
import re
# all_movies_imdb crawl on omdb api
imdb_m = sc.textFile("hdfs:///user/felizxia/si618f17_felizxia_project/allmovies_imdb.json")
# twitter ratings are from github
twitter_r = sc.textFile("hdfs:///user/felizxia/si618f17_felizxia_project/twitter_ratings.txt")
# Normalized  time

def t_date_tf_y(x): # tranform unix time to year
    return datetime.datetime.utcfromtimestamp(int(x)).year

def i_date_tf(x): #  solve year  out of range turn to ISO format--> string
    import calendar
    import datetime
    d = time.strptime(x, "%d %b %Y")[:6]
    year = d[0]
    month = d[1]
    day = d[2]
    hour = d[3]
    minute = d[4]
    dt = datetime.datetime(year, month, day, hour, minute)
    iso = dt.isoformat()
    # time_format = datetime.datetime.strptime(iso,"%Y-%m-%dT%H:%M:%S")
    return iso
# Load data
twitter = twitter_r.map(lambda r:r.split('::'))
# rating_tw for visualization reason, we use year as date transform
rating_tw=twitter.map(lambda id: [id[0],'tt'+str(id[1]),id[2],t_date_tf_y(id[3])]).toDF()
twitter_data=rating_tw.selectExpr("_1 as Twitter_UserID","_2 as imdbID","_3 as twitterRating", "_4 as twitterTime")
twitter_data.registerTempTable('twitter_data')

def all_movies(data):
    all_list = []
    title = data.get('Title', None)
    id = data.get('imdbID',None)
     #fromtimestamp for python2.7 pysaprk side, timestamp for python3
    i_rating = data.get('imdbRating',None)
    genres = data.get('Genre',None)
    genre= re.split('\W+',genres)[0]
    year= data.get('Year',None)
    released  = data.get('Released', None)
    BoxOffice = data.get('BoxOffice',None)
    country = data.get('Country',None)
    r_count = data.get('imdbVotes',None)
    if released != 'N/A':
        released = i_date_tf(released)
        all_list.append((id,title,genre,i_rating,year, country,r_count,BoxOffice, released))
    return all_list

movie_data = imdb_m.map(lambda line: json.loads(line)).flatMap(all_movies).toDF().cache()
movie_imdb= movie_data.selectExpr("_1 as imdbID","_2 as Title", "_3 as Genre","_4 as imdbRating","_5 as Year", "_6 as Country","_7 as imdb_Votes","_8 as ticket_sales","_9 as released_date")
movie_imdb.registerTempTable('movie_imdb')

# Dataset exploration

twitter_data.describe().show()
movie_imdb.describe().show()

# movie counts, movie genres, time range

e1= sqlContext.sql('select count(DISTINCT twitter_data.imdbID) from twitter_data JOIN movie_imdb on twitter_data.imdbID=movie_imdb.imdbID').first()

e2= sqlContext.sql('select count(DISTINCT imdbID) from movie_imdb').first()

e3= sqlContext.sql('select sum(imdb_Votes) from movie_imdb').first()
# analysis
# twitter average rating score and counts  by genre
analysis1= sqlContext.sql('select Genre, sum(twitterRating)/count(twitterRating) AS twitterAvgRating from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID group by movie_imdb.Genre order by twitterAvgRating DESC')
analysis1.registerTempTable('a1')
analysis1.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitter_avg_ratings_by_genre.csv')
analysis3= sqlContext.sql('select Genre, count(twitterRating) AS twitterVotes from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID group by movie_imdb.Genre order by twitterVotes')
analysis3.registerTempTable('a3')
analysis3.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitter_most_rated_genres.csv')
t1= sqlContext.sql('select Genre, twitterVotes, TwitterAvgRating from analysis1 JOIN analysis3 on Genre groupby Genre order by TwitterAvgRating,twitterVotes')


# imdb average ratings score by genre
analysis2 = sqlContext.sql('select Genre, sum(imdbRating)/count(imdbRating) AS imdbAvgRating from movie_imdb group by Genre order by imdbAvgRating DESC')
analysis2.collect()
analysis2.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/imdb_avg_ratings_by_genre.csv')
# votes (popularity of different genres) in imdb
analysis4 = sqlContext.sql('select Genre, sum(imdb_Votes) AS imdbVotes from movie_imdb group by Genre order by imdbVotes DESC')
analysis4.collect()
analysis4.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/imdb_most_rated_genres.csv')

# would twitter users vote for most recently movies or by old movies

analysis5 = sqlContext.sql('select (twitter_data.twitterTime - movie_imdb.Year) ,count(twitter_data.twitterTime - movie_imdb.Year) AS TwitterVotes from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID group by (twitter_data.twitterTime - movie_imdb.Year),twitter_data.imdbID Order by TwitterVotes DESC')analysis5.collect()
analysis5 = analysis5.selectExpr('_c0 as TimePeriod', 'TwitterVotes as TwitterVotes')
analysis5.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitter_voting_preffered_Time.csv')

## What type of movies they voted most abd their ratings change through year in twitter
analysis6 = sqlContext.sql('select twitterTime,Genre, count(twitterRating),sum(twitterRating)/count(twitterRating) As twitterAvgRating from twitter_data JOIN movie_imdb ON twitter_data .imdbID= movie_imdb.imdbID group by twitterTime,Genre order by twitterAvgRating,count(twitterRating) DESC')
analysis6.collect()
analysis6 = analysis6.selectExpr('twitterTime as twitter_Voting_Time','Genre as Genre','_c2 as Votes')
analysis6.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitter_rating_preffered_Genre_AcrossTime.csv')

## What are twitter’s users attitude to movies produced in different countries? Will they be more tolerant, showing more positive reviews as compare to the same genre in specific countries?
# first explore imdb movies produced in different countries
## imdb country movie ratio
e4 = sqlContext.sql('select Country, count(imdbID)/26036 as imdb_percentage from movie_imdb group by Country order by imdb_percentage DESC LIMIT 10')
## Twitter users prefer movies produced in movies produced by multiple countries

analysis7 = sqlContext.sql('select Country, count(twitter_data.imdbID)/649320 as twitter_precentage from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID group by Country order by twitter_precentage DESC LIMIT 10')
analysis7.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitter_rating_preffered_Countries_ratio.csv')
## imdb averagr rating per country
e5 = sqlContext.sql('select Country, sum(imdbRating)/count(imdbRating) as Ratings from movie_imdb where group by Country order by Ratings DESC')
x= e5.rdd.map(lambda x: [x[0].split(),x[1]])\
.filter(lambda c:len(c[0])==1).collect()

e7=sqlContext.sql('select Country, sum(twitterRating)/ count(twitterRating) as Ratings from twitter_data JOIN movie_imdb ON  twitter_data.imdbID= movie_imdb.imdbID group by Country order by Ratings DESC LIMIT 10')

## twitter rating vs box office sales
# rating counts relationship with BoxOffice Sales
analysis8 = sqlContext.sql('select count(twitterRating) AS twitterVotes, ticket_sales, sum(twitterRating)/count(twitterRating) AS twitterAvgRating from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID where ticket_sales != "N/A" group by  ticket_sales order by twitterVotes DESC')
analysis8.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitterVotes_with_BoxOfficeSales.csv')

## rating score relationship with BoxOffice Sales

analysis9 = sqlContext.sql('select sum(twitterRating)/count(twitterRating) AS twitterAvgRating, ticket_sales, count(twitterRating) AS twitterVotes from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID where ticket_sales != "N/A" group by  ticket_sales order by twitterAvgRating DESC')
analysis9.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/twitterAvgRatings_with_BoxOfficeSales.csv')


## how 8,9 group by genres:
analysis10 = sqlContext.sql('select count(twitterRating) AS twitterVotes, ticket_sales,Genre, sum(twitterRating)/count(twitterRating) AS twitterAvgRating from twitter_data JOIN movie_imdb ON twitter_data.imdbID= movie_imdb.imdbID where ticket_sales != "N/A" group by ticket_sales,Genre order by twitterVotes DESC')
analysis10.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/test.csv')
# how frequently would users rate on twitter?

analysis11= sqlContext.sql('select Year,sum(imdb_Votes) AS IMDBVotes, ticket_sales,Genre, sum(imdbRating)/count(imdbRating) AS IMDBAvgRating from movie_imdb where ticket_sales != "N/A" group by ticket_sales, Genre, Year order by IMDBVotes DESC')
# filter None Value
analysis11 = analysis11.filter("IMDBVotes is not NULL")

analysis11.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/test2.csv')


## Time series analysis
# filter movie_imdb data
def i_t(x): #  solve year  out of range turn to ISO format--> string
    time_format = datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%S")
    return time_format
movie_imdb_recent = sqlContext.sql('select * from movie_imdb where Year >= 2013')
imdb=movie_imdb_recent.map(lambda line: [line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],i_b(line[8])]).toDF().cache()
imdb= imdb.selectExpr("_1 as imdbID","_2 as Title", "_3 as Genre","_4 as imdbRating","_5 as Year", "_6 as Country","_7 as imdb_Votes","_8 as ticket_sales","_9 as released_date")
imdb.registerTempTable('imdb')

# transform twitter data to datetime dataframe
def t_date_tf(x): # tranform unix time to ISO format in order to calculate this with imdb date
    return datetime.datetime.utcfromtimestamp(int(x))

twitter_time = twitter.map(lambda x:[x[0],'tt'+str(x[1]),x[2],t_date_tf(x[3])]).toDF().cache()
twitter_time=twitter_time.selectExpr("_1 as Twitter_UserID","_2 as imdbID","_3 as twitterRating", "_4 as twitterTime")
twitter_time.registerTempTable("twitter_t")

# do twitter votes time series analysis

t1= sqlContext.sql('select * from imdb JOIN twitter_t ON imdb.imdbID = twitter_t.imdbID') # join two datasets together

# use map to get one as their time period
#imdbID: string, Title: string, Genre: string, imdbRating: string, Year: string, Country: string, imdb_Votes: string, ticket_sales: string
# released_date: timestamp, Twitter_UserID: string, imdbID: string, twitterRating: string, twitterTime: timestamp
t_final = t1.rdd.map(lambda x:[x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],(x[12]-x[8]).days,x[11],x[12]]).toDF().cache()
t_final= t_final.selectExpr("_1 as imdbID","_2 as Title", "_3 as Genre","_4 as imdbRating","_5 as Year", "_6 as Country","_7 as imdb_Votes","_8 as ticket_sales","_9 as released_date","_10 as Twitter_UserID","_11 as time_period","_12 as twitterRating", "_13 as twitterTime")
t_final.registerTempTable('twitter_imdb')

#where (twitterTime - released_date).days <8')
time_vote = sqlContext.sql('select Year, time_period, count(imdbRating) AS TotalVotes from twitter_imdb group by time_period, Year order by TotalVotes DESC')
time_vote.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/time_vote.csv')
time_vote_total = sqlContext.sql('select time_period, count(imdbRating) AS TotalVotes from twitter_imdb group by time_period order by TotalVotes DESC')
time_vote_total.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/time_vote_totals.csv')

# check this per genre
genres = sqlContext.sql('select Genre, sum(twitterRating)/count(twitterRating) AS twitterAvgRating, count(imdbRating) AS TotalVotes, time_period from twitter_imdb  group by Genre, time_period order by TotalVotes DESC')
genres.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/genres_crosstime.csv')
# check for most popular Action Movies, how twitter's rating change aross time
action = sqlContext.sql('select Title, sum(twitterRating)/count(twitterRating) AS twitterAvgRating, count(imdbRating) AS TotalVotes, time_period from twitter_imdb where Genre="Action"  group by Title, time_period order by TotalVotes DESC LIMIT 1000')
action.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/action_movie_rate_crosstime.csv')

# compare bio with preview and without preview

bio =  sqlContext.sql('select Title, sum(twitterRating)/count(twitterRating) AS twitterAvgRating, time_period from twitter_imdb where Genre="Biography" AND time_period<0 group by Title, time_period')
bio.registerTempTable('bio')
bio= bio.selectExpr("Title as bio_title","_2 as bio_avgTwitter","_3 as bio_time", "_4 as twitterTime")

biography= sqlContext.sql('select Title,sum(twitterRating)/count(twitterRating) AS twitterAvgRating, time_period from twitter_imdb where Genre="Biography" group by Title, time_period')
biography.registerTempTable('biography')

# see bio'rating with perview score compare to overall average score
name_list = bio.rdd.map(lambda x:x[0]).collect()
filter_bio=biography.where(col('Title').isin(name_list))
filter_bio.registerTempTable("filter_bio")
filter_bio.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/withPreview_Bio.csv')
# with preview's movies' have avg score of 7.527802159507689
filter_bio_score = sqlContext.sql('select sum(twitterAvgRating)/count(twitterAvgRating) AS previewAvgRating from filter_bio')

# without preview's
bio_no= sqlContext.sql('select Title,sum(twitterRating)/count(twitterRating) AS twitterAvgRating from twitter_imdb where Genre="Biography" AND time_period>0 group by Title')
bio_no.registerTempTable("bio_no")
bio_no_score = sqlContext.sql('select sum(twitterAvgRating)/count(twitterAvgRating) as nopreviewwAvgRating from bio_no')
# average only 7.132364885279287

biography.rdd.map(lambda row: '\t'.join(str(i) for i in row)) \
    .saveAsTextFile('/user/felizxia/si618f17_felizxia_project/biography_movie_rate_crosstime.csv')


