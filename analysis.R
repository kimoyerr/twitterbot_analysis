library(mongolite)

con <- mongo("crispr_tweets", url = "mongodb+srv://kimoyerr:mohan921983@tweetdb-1f6vz.mongodb.net/tweets")
con$count()

library(dplyr) # Always load dplyr before sparkR
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
head(select(df, "tweet_created"))
head(count(groupBy(df, df$user_name)),100)
# Create a new day field
df = df %>% withColumn("day", to_date(.$tweet_created,  "ddMMMyy"))
df_local = SparkR::collect(df)

# Change \n to spaces because sparklyr cannot create dataframes otherwise
df_local$tweet_text <- gsub('\n', ' ', df_local$tweet_text)
df_local$tweet_loc <- gsub('\n', ' ', df_local$tweet_loc)

gen = count(groupBy(df, df$day))
printSchema(gen)
tmp = SparkR::collect(gen)


library(sparklyr)
config <- spark_config()
config["sparklyr.shell.driver-memory"] <- "8g"
sc <- sparklyr::spark_connect(master = "local[*]", 
                              spark_home = Sys.getenv("SPARK_HOME"),config = config)
tmp_tbl <- copy_to(sc, tmp, overwrite = T)
df_tbl <- sparklyr::copy_to(sc, df_local, overwrite = T)
src_tbls(sc)

#NLP
df_tbl <- df_tbl %>%
  dplyr::mutate(tweet_text = regexp_replace(tweet_text, "[_\"\'():;,.!?\\-]|//t|https", " "))
df_tbl <- df_tbl %>%
  ft_tokenizer(input_col = "tweet_text",
               output_col = "word_list")

head(df_tbl, 4)
df_tbl <- df_tbl %>%
  ft_stop_words_remover(input_col = "word_list",
                        output_col = "wo_stop_words")
head(df_tbl, 4)
tmp <- df_tbl %>%
  dplyr::mutate(word = explode(wo_stop_words)) %>% 
  dplyr::select(word, day) %>%
  dplyr::filter(nchar(word) > 2) %>%
  compute("all_words")

word_count <- tmp %>%
  dplyr::group_by(word) %>%
  dplyr::tally() %>%
  dplyr::filter(!word %in% c('crispr',"gene","#crispr",'editing'))  %>% 
  dplyr::arrange(desc(n))
word_count


word_count %>% head(200) %>%
  collect() %>%
  with(wordcloud::wordcloud(
    word, 
    n,
    colors = c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7"),
    ordered.colors=FALSE, random.color = TRUE, random.order=FALSE,
    c(4,0.5)))

spark_disconnect(sc)



