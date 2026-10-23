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


# ---------------------------------------------------------------- Step 0: load and register
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


# ---------------------------------------------------------------- Task 1 (DataFrame API)
def task1_hashtag_trends():
    """The ten most used hashtags.

    The hashtags column holds a comma-separated list such as "#tech,#AI". Split it, explode
    it into one row per hashtag (slide "Complex Types: Explode and Collect"), normalize each
    hashtag with lower() and trim() so that #Tech, #TECH and #tech count as one, then count.
    Columns: hashtag, count. The 10 most frequent, ordered by count descending, then hashtag.
    """
    # TODO
    return None


# ---------------------------------------------------------------- Task 2 (Spark SQL)
def task2_engagement_by_age_sql():
    """Average engagement per age group, in ONE spark.sql(...) call over the views.

    Join posts with users on user_id and group by age_group.
    Columns: age_group, posts (number of posts), avg_likes, avg_retweets (both rounded
    to 2 decimals). Ordered by avg_likes descending.
    """
    # TODO
    return None


def task2_engagement_by_age_df():
    """The same result as task2_engagement_by_age_sql, written with the DataFrame API
    (join, groupBy, agg, orderBy). Both plans are printed by main() for your report.
    """
    # TODO
    return None


# ---------------------------------------------------------------- Task 3 (Spark SQL)
def task3_sentiment_vs_engagement_sql():
    """Does sentiment pay? Engagement per sentiment category, in ONE spark.sql(...) call.

    Categorize each post with a CASE expression on sentiment_score (slide "Query Data:
    Expressions in SQL"): 'Positive' if the score is greater than 0.3, 'Negative' if it is
    less than -0.3, 'Neutral' otherwise.
    Columns: sentiment, posts, avg_likes, avg_retweets (both rounded to 2 decimals).
    Ordered by sentiment (so Negative, Neutral, Positive).
    """
    # TODO
    return None


# ---------------------------------------------------------------- Task 4 (DataFrame API)
def task4_top_verified_users():
    """The five verified users with the largest reach.

    reach = total likes + total retweets over all of the user's posts. Only verified users.
    Columns: username, total_likes, total_retweets, reach. The top 5 by reach descending,
    then username.
    """
    # TODO
    return None


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
