library(ggplot2)
require(data.table)
setwd('/Users/rita/Google Drive/618/618_project/visualization')
# Load IMDB most rated data and each genres average ratings
read_file = function(x)
{
  read.table(x, sep='\t', quote = "", row.names = NULL,  comment.char = "",fill = TRUE,fileEncoding = "UTF-8-BOM")
}

imdb_g  = read_file('imdb_most_rated_genres.csv')
imdb_r = read_file('imdb_avg_ratings_by_genre.csv')
twitter_g = read_file('twitter_most_rated_genres.csv')
twitter_r = read_file('twitter_rating_preffered_Genre_AcrossTime.csv')
twitter_r_a= read_file('twitter_avg_ratings_by_genre.csv')

# rename columns
colnames(imdb_g) = c('Genre','TotalVotes')
colnames(imdb_r) = c('Genre','Ratings')
colnames(twitter_g) = c('Genre','Votes')
colnames(twitter_r) = c("Year","Genre","Votes","Ratings")
colnames(twitter_r_a)= c("Genre","Ratings")
# data.table JOIN https://rstudio-pubs-static.s3.amazonaws.com/52230_5ae0d25125b544caab32f75f0360e775.html
# change from dataframe to data.table
setDT(imdb_g)
setDT(imdb_r)
setDT(twitter_g)
setDT(twitter_r)
setDT(twitter_r_a)
# set all keys inorder to join them
setkey(imdb_g,Genre)
setkey(imdb_r,Genre)
setkey(twitter_r_a,Genre)
setkey(twitter_g,Genre)

# order imdb_g_r by Votes
imdb_g_v = imdb_g_r[order(-TotalVotes)]
# order twitter_f_a by Votes
twitter_r_o = twitter_r_f[order(-Votes)]
twitter_r_o= twitter_r_o[-c(25),]
# remove unknow 'N' from data
imdb_g_v = imdb_g_v[-c(22),]
# twitter ratings
twitter_r_a = twitter_r_a[-c(17),]
# ordered by ratings
twitter_r_a = twitter_r_a[order(-Ratings),]
# twitter votes
twitter_g = twitter_g[-c(24),]


# Join two tables based on keys (table with both votes and ratings)
imdb_g_r = imdb_g[imdb_r,nomatch=0]  # imdb votes and ratings
twitter_r_f = twitter_r_a[twitter_g,nomatch=0] # twitter votes and ratings

#@@ Visualization
# Plot most rated genres in IMDB descending order
ggplot(imdb_g_v,aes(x=imdb_g_v$Genre))+geom_bar(aes(x=imdb_g_v$Genre,y=imdb_g_v$TotalVotes,fill= Genre == c('Drama','Comedy','Documentary','Action','Horror')),stat="identity",alpha=0.6) + scale_x_discrete(limits= c('Drama','Comedy','Documentary','Action','Horror','Crime','Adventure','Short','Animation','Thriller'))+ggtitle("Most viewed Genres in IMDB", subtitle = NULL)+xlab('Genre')+ylab('IMDB Total Votes')+ theme(legend.position = "none")

# plot imdb most highly rated genres descending order 
# Order imdb_g_r by Ratings
imdb_g_rat = imdb_g_r[order(-Ratings)]
imdb_g_rat = imdb_g_rat[-c(26),]
ggplot(imdb_g_rat,aes(x=imdb_g_rat$Genre))+geom_line(aes(y=imdb_g_rat$Ratings,group=1),color='pink',lwd=3,alpha=0.6) + geom_point(aes(y=imdb_g_rat$Ratings))+scale_x_discrete(limits= c('Film','Documentary','Biography','Animation','War','History','Drama','Western','Crime','Musical','Sport','Adventure','Mystery','Comedy','Music','Romance','Family','Action','Fantasy','Thriller','Short','Adult','Horror','Sci','Game'))+ggtitle("Preffered Genres in IMDB", subtitle = NULL)+xlab('Genre')+ylab('IMDB Ratings')+theme(axis.text.x = element_text(angle = 60, hjust =1))
                                                    
# plot imdb most rated genres and their ratings
ggplot(imdb_g_v,aes(x=imdb_g_v$Genre))+geom_bar(aes(y=log(imdb_g_v$TotalVotes),fill=Genre == c('Action','Comedy','Drama','Horror')),fill="grey",alpha=0.7,stat="identity")+geom_line(aes(x=imdb_g_v$Genre,y=imdb_g_v$Ratings,group=1,color="IMDB Ratings"),linetype=1,lwd=1,alpha=0.5)+geom_point(aes(y=imdb_g_v$Ratings))+geom_line(aes(x=imdb_g_v$Genre,y=log(imdb_g_v$TotalVotes),color="Log(IMDB Total Votes)",group=1),linetype=2,lwd=1,alpha=0.5)+geom_line(aes(x=imdb_g_v$Genre,y=imdb_g_v$Ratings,group=1),linetype=2,color='red',lwd=1,alpha=0.5)+geom_point(aes(y=log(imdb_g_v$TotalVotes)))+theme(axis.text.x = element_text(angle = 60, hjust =1))+ ggtitle("IMDB Genre Votes Counts vs Genre Average Ratings", subtitle = NULL)+xlab('Genre')+ylab('Log(Total Votes Counts)')

# Plot most rated genres of twitter descending order
ggplot(twitter_g,aes(x=twitter_g$Genre))+geom_bar(aes(x=twitter_g$Genre,y=twitter_g$Votes,fill= Genre == c('Action','Drama','Comedy','Crime','Biography')),stat="identity",alpha=0.6) + scale_x_discrete(limits= c('Action','Drama','Comedy','Crime','Biography','Adventure','Animation','Horror','Documentary','Mystery'))+ggtitle("Most viewed Genres in Twitter", subtitle = NULL)+xlab('Genre')+ylab('Twitter Total Votes')+ theme(legend.position = "none")

#plot twitter most highly rated genres descending order 

# remove outliers Game
twitter_r_a = twitter_r_a[-c(25),]
ggplot(twitter_r_a,aes(x=twitter_r_a$Genre))+geom_line(aes(y=twitter_r_a$Ratings,group=1),color='pink',lwd=3,alpha=0.6) + geom_point(aes(y=twitter_r_a$Ratings))+ggtitle("Preffered Genres in Twitter", subtitle = NULL)+xlab('Genre')+ylab('Twitter Ratings')+theme(axis.text.x = element_text(angle = 60, hjust =1))+ scale_x_discrete(limits= c("Western","Short","Sport","Documentary","Biograghy","War","Film","Animation","Crime","Adventure","Drama", "Family","Mystery","Adult","Musical","History","Sci","Comedy","Action","Romance","Fantasy","Horror","Thriller","Music")
                             
                             
# order twitter r with descdending order (ordered by year)
twitter_r = twitter_r[order(-Ratings)]
# Plot rating score changes of genres of twitter  through time
Year= twitter_r$Year
ggplot(twitter_r,aes(x=twitter_r$Genre))+geom_point(aes(y=twitter_r$Ratings,color=Year))+ scale_x_discrete(limits= c('Action','Drama','Comedy','Crime','Biography','Adventure','Animation','Horror','Documentary','Mystery'))+scale_y_continuous(limits = c(6.2, 8))+scale_color_manual(values=wes_palette(n=5, name="Rushmore"))+ ggtitle("Twitter Ratings of different Genres from 2013--2017", subtitle = NULL)+xlab('Genre')+ylab('Twitter Total Votes')

## plot twitter most rated genres and their ratings

ggplot(twitter_r_o,aes(x=twitter_r_o$Genre))+geom_bar(aes(y=log(twitter_r_o$Votes)),fill="grey",alpha=0.7,stat="identity")+geom_line(aes(x=twitter_r_o$Genre,y=twitter_r_o$Ratings,group=1,color="Twitter Ratings"),linetype=1,lwd=1,alpha=0.5)+geom_point(aes(y=twitter_r_o$Ratings))+geom_line(aes(x=twitter_r_o$Genre,y=log(twitter_r_o$Votes),color="Log(Twitter Total Votes)",group=1),linetype=2,lwd=1,alpha=0.5)+geom_point(aes(y=log(twitter_r_o$Votes)))+theme(axis.text.x = element_text(angle = 60, hjust =1))+ ggtitle("twitter Genre Votes Counts vs Genre Average Ratings", subtitle = NULL)+xlab('Genre')+ylab('Log(Total Votes Counts)'))


# final visualization
votes = read.table('twitterVotes_with_BoxOfficeSales.csv', sep='\t', quote = "", row.names = NULL,  comment.char = "",fill = TRUE,fileEncoding = "UTF-8-BOM")
ratings= read.table('twitterAvgRatings_with_BoxOfficeSales.csv', sep='\t', quote = "", row.names = NULL,  comment.char = "",fill = TRUE,fileEncoding = "UTF-8-BOM")
colnames(votes)<- c("Votes","TicketSales","twitter_ratings")
colnames(ratings)<- c("twitter_ratings","TicketSales","Votes")
# transform them to data.table

setDT(votes)
setDT(ratings)
setDT(test)
votes_g= transform(votes,rank= ave(votes$Votes, FUN=function(x) order(x,decreasing = T)))
ratings_g =  transform(ratings,rank= ave(ratings$twitter_ratings, FUN=function(x) order(x,decreasing = T)))
# get rid of symbols remember to add \\ to escape special letters!!!!! $
votes_g$TicketSales = gsub("\\$",'',votes_g$TicketSales)
votes_g$TicketSales = gsub(',','',votes_g$TicketSales)
votes_g$TicketSales = as.numeric(votes_g$TicketSales)
ratings_g$TicketSales = gsub("\\$",'',ratings_g$TicketSales)
ratings_g$TicketSales = gsub(',','',ratings_g$TicketSales)
ratings_g$TicketSales = as.numeric(ratings_g$TicketSales)

# see genres that people rate a lot but give high rating scores

ggplot(imdb_g_r,aes(x=imdb_g_r$Genre))+geom_bar(aes(y=log(imdb_g_r$TotalVotes)),fill="grey",alpha=0.7,stat="identity")+geom_line(aes(x=imdb_g_r$Genre,y=imdb_g_r$Ratings,group=1),color='red',lwd=1.5,alpha=0.5)+geom_point(aes(y=imdb_g_r$Ratings))+geom_line(aes(x=imdb_g_r$Genre,y=log(imdb_g_r$TotalVotes),group=1),color="black",lwd=1.5,alpha=0.5)ggplot(imdb_g_r,aes(x=imdb_g_r$Genre))+geom_bar(aes(y=log(imdb_g_r$TotalVotes)),fill="grey",alpha=0.4,stat="identity")+geom_line(aes(x=imdb_g_r$Genre,y=imdb_g_r$Ratings,group=1),color='red',lwd=1.5,alpha=0.5)+geom_point(aes(y=imdb_g_r$Ratings))+geom_line(aes(x=imdb_g_r$Genre,y=log(imdb_g_r$TotalVotes),group=1),color="black",lwd=1.5,alpha=0.5)ggplot(imdb_g_r,aes(x=imdb_g_r$Genre))+geom_bar(aes(y=log(imdb_g_r$TotalVotes)),fill="grey",alpha=0.4,stat="identity")+geom_line(aes(x=imdb_g_r$Genre,y=imdb_g_r$Ratings,group=1),color='red',lwd=1.5,alpha=0.5)+geom_point(aes(y=imdb_g_r$Ratings))+geom_line(aes(x=imdb_g_r$Genre,y=log(imdb_g_r$TotalVotes),group=1),color="black",lwd=1.5,alpha=0.5)

ggplot(imdb_g_r,aes(x=imdb_g_r$Genre))+geom_bar(aes(y=log(imdb_g_r$TotalVotes)),fill="grey",alpha=0.7,stat="identity")+geom_line(aes(x=imdb_g_r$Genre,y=imdb_g_r$Ratings,group=1),color='red',lwd=1.5,alpha=0.5)+geom_point(aes(y=imdb_g_r$Ratings))+geom_line(aes(x=imdb_g_r$Genre,y=log(imdb_g_r$TotalVotes),group=1),color="black",lwd=1.5,alpha=0.5)


ggplot(votes_g,aes(x=votes_g$rank,y=votes_g$TicketSales,color=votes_g$twitter_ratings))+geom_point()+geom_smooth(method= loess, color="orange", fill="yellow")+ggtitle('Tiwtter TotalVotes & TicketSales') + xlab('TotalVotes Rank') + ylab('TicketSales')+theme(legend.position='None')
ggplot(ratings_g,aes(x=ratings_g$rank,y=ratings_g$TicketSales))+geom_point()+geom_smooth(method="loess",color="yellow")+ggtitle('Tiwtter Average Ratings & TicketSales') + xlab('Twitter Average Ratings Rank') + ylab('TicketSales')+theme(legend.position='None')

#Time series Analysis
# overall situation
tw_time_t = read_file("time_vote_totals.csv")
colnames(tw_time_t) = c("Time Peroid","Total Votes")
setDT(tw_time_t)
ggplot(tw_time_t,aes(x=tw_time_t$`Time Peroid`,y=tw_time_t$`Total Votes`))+geom_point() + ggtitle("Twitter User rating time vs movie released time")+xlab("Rating Time Period")+ylab("Total Votes")

ggplot(tw_time_t,aes(x=tw_time_t$`Time Peroid`))+geom_histogram(binwidth = 200) + ggtitle("Twitter User rating time vs movie released time")+xlab("Rating Time Period")+ylab("Total Votes")


#see how this different for each genres
genres = read_file('genres_crosstime.csv')
colnames(genres) = c("Genres","Total Votes","Time Peroid")
setDT(genres)

ggplot(genres,aes(x=genres$`Time Peroid`,y=genres$`Total Votes`,color=genres$Genres))+geom_point() +ggtitle("Twitter User rating score for Action Movie change trend")+xlab("Rating Time Period")+ylab("Avg Ratings")+scale_x_discrete(limits =c("Action","Drama","Adventure","Documentary","Biography","Horror"))

# see action movie's rating changes across time
action = read_file("action_movie_rate_crosstime.csv")

colnames(action) = c("Title","Avg Ratings","Total Votes","Time Peroid")
setDT(action)
# see action movie has a rating trend.. most people love rate most recent action movies

ggplot(action,aes(x=action$`Time Peroid`,y=action$`Total Votes`,color=action$Title))+geom_point() + scale_x_continuous(limits=c(0,200))+ggtitle("Twitter User rating score for Action Movie change trend")+xlab("Rating Time Period")+ylab("Total Votes")+theme(legend.position='None')

# for most popular action movie top 5, see their rating trend changes 
action1 = action[action$Title==c("Man of Steel","Godzilla","Suicide Squad","Iron Man 3","Jurassic World"),]
ggplot(action1,aes(x=action1$`Time Peroid`,y=action1$`Avg Ratings`,color=action1$`Total Votes`))+geom_line() +ggtitle("Twitter User rating score for Biography Movie change trende")+xlab("Rating Time Period")+ylab("Avg Ratings")

# trend 
xyplot(action$`Avg Ratings`~log(action$`Time Peroid`), test_g,grid=TRUE,type=c('p','smooth'),col.line='red',lwd=4,alpha=0.5,xlab="Time range",ylab="Avg Score")
# explore biograghy data
bio = read_file("biography_movie_rate_crosstime.csv")

colnames(bio) = c("Title","Avg Ratings","Total Votes","Time Peroid")
setDT(bio)

# general trend 
ggplot(bio,aes(x=bio$`Time Peroid`,y=bio$`Avg Ratings`,color=bio$Title))+geom_point() +ggtitle("Twitter User rating time vs movie released time")+xlab("Rating Time Period")+ylab("Total Votes")+theme(legend.position='None')

# trend
xyplot(bio$`Avg Ratings`~log(bio$`Time Peroid`), test_g,grid=TRUE,type=c('p','smooth'),col.line='red',lwd=4,alpha=0.5,xlab="Time range",ylab="Avg Score")


# explore biography with previews score change

bio_preview = read_file("withPreview_Bio.csv")
setDT(bio_preview)
colnames(bio_preview) =c("Title","Avg Ratings","Time Peroid")
ggplot(bio_preview,aes(x=bio_preview$`Time Peroid`,y=bio_preview$`Avg Ratings`,color=bio_preview$Title))+geom_point() +ggtitle("Twitter User rating time vs movie released time")+xlab("Rating Time Period")+ylab("Total Votes")+theme(legend.position='None')

# explore relationship of twitter Votes counts with ticket sales grouped by genres
test = read.table('test.csv', sep='\t', quote = "", row.names = NULL,  comment.char = "",fill = TRUE,fileEncoding = "UTF-8-BOM")
colnames(test) = c('TotalVotes','TicketSales','Genre','Ratings')
test$TicketSales = gsub("\\$",'',test$TicketSales)
test$TicketSales = gsub(',','',test$TicketSales)
test$TicketSales = as.numeric(test$TicketSales)
# remove insigniicant levels
test_g= test[test$Genre %in% c('Action','Adventure','Biography','Animation'),]
test_g = droplevels(test_g)
#plot multip scales gragh using xyplot on log-log scale
library(lattice)
xyplot(log(test_g$TicketSales)~test_g$TotalVotes  | Genre, test_g,grid=TRUE,scales = list(x = list(log = 10, equispaced.log = FALSE)),type=c('p','smooth'),col.line='red',lwd=4,alpha=0.5,xlab="Total Votes",ylab="Log(Ticket Sales)")

# compare total votes and ratings
xyplot(log(test_g$TicketSales)~log(test_g$TotalVotes) + log(test_g$Ratings) | Genre, test_g,grid=TRUE,scales = list(x = list(log = 10, equispaced.log = FALSE)),xlab="Total Votes",ylab="Log(Ticket Sales)")

# compare this IMDB's votes and its' ticket sales

testIMDB = read_file('test2.csv')
colnames(testIMDB) = c('TotalVotes','TicketSales','Genre','Ratings')
testIMDB$TicketSales = gsub("\\$",'',testIMDB$TicketSales)
testIMDB$TicketSales = gsub(',','',testIMDB$TicketSales)
testIMDB$TicketSales = as.numeric(testIMDB$TicketSales)
test_i= testIMDB[testIMDB$Year %in% c(20),]


xyplot(log(testIMDB$TicketSales)~testIMDB$TotalVotes | Genre, testIMDB,grid=TRUE,scales = list(x = list(log = 10, equispaced.log = FALSE)),type=c('p','smooth'),col.line='red',lwd=4,alpha=0.5,xlab="Total Votes",ylab="Log(Ticket Sales)")
