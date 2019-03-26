library(mongolite)

con <- mongo("crispr_tweets", url = "mongodb+srv://kimoyerr:mohan921983@tweetdb-1f6vz.mongodb.net/tweets")
con$count()

Sys.setenv(SPARK_HOME = '/usr/lib/spark')
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
my_spark <- sparkR.session(
  master="local[*]",
  sparkConfig=list(spark.mongodb.input.uri= 'mongodb+srv://kimoyerr:mohan921983@tweetdb-1f6vz.mongodb.net/tweets.crispr_tweets?readPreference=primaryPreferred',
                   spark.mongodb.output.uri= 'mongodb+srv://kimoyerr:mohan921983@tweetdb-1f6vz.mongodb.net/tweets.crispr_tweets'),
  sparkPackages = c('org.mongodb.spark:mongo-spark-connector_2.11:2.4.0'),
  appName="my_app"
)

df <- read.df("", source = "com.mongodb.spark.sql.DefaultSource")
printSchema(df)
