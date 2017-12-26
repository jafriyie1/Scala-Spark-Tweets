# Scala-Spark-Tweets
A Spark pipeline that streams tweets into a MongoDB collection, pulls the data, and then conduct sentiment analysis on tweets.
Hello! This is my attempt at making a full on data science pipeline. I first started by collecting tweets (mainly around bitcoin) about technology
and streaming them from the Twitter API, then to Spark, and then into my local MongoDB database. I then will read the collection from
MongoDB into a Python script to conudct sentiment analysis on the tweets.
