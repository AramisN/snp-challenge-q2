Question 2: 
In this question, you should build an ETL pipeline that extracts two datasets in JSON format from a data lake hosted on MINIO, then processes them using Spark SQL, and load the data back in  MINIO as CSV files. The datasets are collected by crawling tweets and users from Twitter.
Note:  Due to the code complexity, we provide the reading and writing code on MINIO in the Appendix section.

Given the user dataset, you are expected to implement the following steps:
1.	Read JSON files from Minio.
2.	Select the following fields; id, id_str, name, screen_name, location, description, url, protected, followers_count, friends_count, listed_count, created_at, favourites_count, statuses_count, lang, profile_image_url_https, timestamp.
3.	Remove duplicate users.
4.	Remove space characters from description, name, location, and URL fields.
5.	Convert created_at field to DateTime with (year-month-day) format.
6.	Load data in MINIO.


Given the tweet dataset, you are expected to implement the following steps:
1.	Read JSON files from Minio.
2.	Remove the user field.
3.	Remove retweeted_status and quoted_status if they are available in JSON objects and add them to dataframe as new rows.
4.	Remove duplicate tweets.
5.	Remove space characters from text fields.
6.	Convert created_at field to DateTime with (year-month-day) format.
7.	Partition dataframe based on created_at date.
8.	Load each partition in separate folders in MINIO. The name of folders should be set according to the partition name.

