"""Hands-on L9: Social Media Analysis with Spark SQL and DataFrames.

Usage (on the Docker cluster from Hands-on L5):
    spark-submit main.py <input directory> <output directory>

Reads posts.csv and users.csv from the input directory and writes one CSV result per task
under the output directory (task1/ ... task4/).

Fill in the parts marked TODO. Each task is a function that returns a DataFrame; a task
whose function still returns None is skipped, so you can run the file after each step.
Tasks 2 and 3 must be written in Spark SQL (one spark.sql call over the views below);
tasks 1 and 4 and the DataFrame twin of task 2 use the DataFrame API.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, sum as ssum, avg, round as sround, desc,
                                   explode, split, lower, trim)

if len(sys.argv) != 3:
    print(__doc__)
    sys.exit(2)
in_dir, out_dir = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")

spark = SparkSession.builder.appName("SocialMediaAnalysis").getOrCreate()


def save(df, name):
    """Print a DataFrame and write it as a single CSV file with a header."""
    if df is None:
        print(f"\n=== {name}: not implemented yet ===")
        return
    print(f"\n=== {name} ===")
    df.show(20, truncate=False)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/{name}")


# ----------------------------------- Step 0: load and register
# Explicit schemas (slides "Define Schema", "CSV"): no inference pass, exact types.
posts_schema = ("post_id STRING, user_id STRING, content STRING, timestamp TIMESTAMP, "
                "likes INT, retweets INT, hashtags STRING, sentiment_score DOUBLE")
users_schema = "user_id STRING, username STRING, age_group STRING, country STRING, verified BOOLEAN"

posts = spark.read.csv(f"{in_dir}/posts.csv", header=True, schema=posts_schema)
users = spark.read.csv(f"{in_dir}/users.csv", header=True, schema=users_schema)

posts.printSchema()
print(f"{posts.count()} posts, {users.count()} users")

# Temporary views, so that the SQL tasks can refer to the data by name (slide "Read Data
# into a Temporary View"). Nothing is copied: a view is the DataFrame plan under a name.
posts.createOrReplaceTempView("posts")
users.createOrReplaceTempView("users")


# ------------------------ Task 1 (DataFrame API)
def task1_hashtag_trends():
    exploded = posts.select(
        explode(split(col("hashtags"), ",")).alias("raw_hashtag")
    )

    result = (
        exploded
        .select(
            lower(trim(col("raw_hashtag"))).alias("hashtag")
        )
        .groupBy("hashtag")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"), col("hashtag").asc())
        .limit(10)
    )

    return result
    

# --------------------------- Task 2 (Spark SQL)
def task2_engagement_by_age_sql():
    query = """
        SELECT
            u.age_group AS age_group,
            COUNT(*) AS posts,
            ROUND(AVG(p.likes), 2) AS avg_likes,
            ROUND(AVG(p.retweets), 2) AS avg_retweets
        FROM posts p
        JOIN users u
            ON p.user_id = u.user_id
        GROUP BY u.age_group
        ORDER BY avg_likes DESC
    """

    return spark.sql(query)
   

def task2_engagement_by_age_df():
    """The same result as task2_engagement_by_age_sql, written with the DataFrame API
    (join, groupBy, agg, orderBy). Both plans are printed by main() for your report.
    """
    result = (
        posts
        .join(users, "user_id")
        .groupBy("age_group")
        .agg(
            count("*").alias("posts"),
            sround(avg("likes"), 2).alias("avg_likes"),
            sround(avg("retweets"), 2).alias("avg_retweets")
        )
        .orderBy(desc("avg_likes"))
    )

    return result


# -------------------------------- Task 3 (Spark SQL)
def task3_sentiment_vs_engagement_sql():
    """Does sentiment pay? Engagement per sentiment category, in ONE spark.sql(...) call.

    Categorize each post with a CASE expression on sentiment_score (slide "Query Data:
    Expressions in SQL"): 'Positive' if the score is greater than 0.3, 'Negative' if it is
    less than -0.3, 'Neutral' otherwise.
    Columns: sentiment, posts, avg_likes, avg_retweets (both rounded to 2 decimals).
    Ordered by sentiment (so Negative, Neutral, Positive).
    """
    
    query = """
        WITH classified AS (
            SELECT
                CASE
                    WHEN sentiment_score > 0.3 THEN 'Positive'
                    WHEN sentiment_score < -0.3 THEN 'Negative'
                    ELSE 'Neutral'
                END AS sentiment,
                likes,
                retweets
            FROM posts
        )
        SELECT
            sentiment,
            COUNT(*) AS posts,
            ROUND(AVG(likes), 2) AS avg_likes,
            ROUND(AVG(retweets), 2) AS avg_retweets
        FROM classified
        GROUP BY sentiment
        ORDER BY sentiment
    """

    return spark.sql(query)



# ---------------------------- Task 4 (DataFrame API)
def task4_top_verified_users():  
    result = (
        users
        .filter(col("verified") == True)
        .join(posts, "user_id")
        .groupBy("username")
        .agg(
            ssum("likes").alias("total_likes"),
            ssum("retweets").alias("total_retweets")
        )
        .withColumn(
            "reach",
            col("total_likes") + col("total_retweets")
        )
        .select("username", "total_likes", "total_retweets", "reach")
        .orderBy(desc("reach"), col("username").asc())
        .limit(5)
    )

    return result



save(task1_hashtag_trends(), "task1")

t2_sql = task2_engagement_by_age_sql()
save(t2_sql, "task2")
t2_df = task2_engagement_by_age_df()
if t2_sql is not None and t2_df is not None:
    print("\n=== task2: physical plan of the SQL version ===")
    t2_sql.explain()
    print("\n=== task2: physical plan of the DataFrame version ===")
    t2_df.explain()                 # paste both plans into your report and compare them

save(task3_sentiment_vs_engagement_sql(), "task3")
save(task4_top_verified_users(), "task4")

spark.stop()
