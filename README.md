# Movie Twittering User and Behaviors
A Comparison of movie ratings in MovieTwittering dataset and IMDB dataset using Spark

# Motivation

The goal of this project is to analyze how twitters ratings different from IMDB’s ratings among different scales, and most importantly, trying to understand users intentions behind MovieTweeting behaviors. By doing that, we can give film companies intuitions of what type of genres are more likely to become popular in social media, why and when is the best time to do movie propagation,and etc.

There are 3 major questions that will be answered in this data report.

1) What are the differences between the most popular and loved genres in IMDB and twitter platforms? Do most voted movies usually get higher ratings?

2) Would twitter users more likely to share their reviews(ratings) for old movies or for most recent movies?

3) Do movie exposure to Twitter boost their ticket sales? In other words, analyze the relationship between BoxOffice with total votes and rating scores.

# Analysis and Visualization

Exploration1 : User preferences to Genre 

1) Documentary and Biography are both being most mentioned and rated high for both IMDB and twitter. Twitter users prefer to share biography movies and IMDB users voted a lot for Documentary.

2) Movie genres that being mentioned more usually not being rated too high.

Exploration 2 :  Twitter Ratings time series analysis

1) Twitter users rating behaviors have timeline effect.  The top rated days with descending order is day(1,2,0,3,9,8,4,5,7,6).

2) For different Genres:

So knowing that twitter users rate biography trailers, I was wondering whether this preview version is the reason that make biography get higher score/reputation.

Thus I compared the movies with trailers and movies with trailers and wanna see their average score differences. Turns out, from Table 7, biography movies companies who publish their trailers in advance and receive twitter users’ rating will gain much more higher rating.

Type									previewAvgRating	NoPreviewAvgRating
Average rating score	7.527802159507689	7.132364885279287

From that, we can reasonably guess biography movies reputation  have close relationship with their media exposure.

Some other interesting insights. We also observe that horror movies are receiving higher and higher ratings through year. And also the same for Action, Comedy and Mystery.  While Adventure movie decrease their ratings quite a lot through time. The most steady high performance candidates are Animation and Documentary.

Exploration 3:  Movies Exposure in twitter relationship with their BoxOffice performance

1) When talking about films exposure in twitter, I mean both the movie TotalVotes count and their average rating scores. From figure, we can see that there is possible relationship between Twitter Votes count with TicketSales, while there is no clear linear relationship between twitter ratings with TicketSales.

2) For different Genres

I then grouped those data with genres and to see if there is correlation between them. As we can see here(figure 9), after using log transformation of TicketSales, there are positive linear regression between BoxOffice with Twitter TotalVotes counts for Animation, Biography, Action and Adventure movie. Among those movies, biography movie has a most obvious linear correlation.
