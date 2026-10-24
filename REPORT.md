# Hands-on L9: Report

**Name: Hardi Koradiya**

**Student ID: 801484363**

**Email:hkoradiy@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801484363

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.


```bash
# generating the input data 
python3 datagen.py 801484363

# Copying the program into spark container
docker cp main.py spark-master:/opt/spark/work-dir/

# Running all the tasks 
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

# Starting the spark container 
docker exec -it spark-master /opt/spark/bin/spark-sql \
  --master spark://spark-master:7077

# creating temporary views in sql terminal
CREATE OR REPLACE TEMPORARY VIEW posts
USING csv OPTIONS (
  path '/opt/spark/work-dir/shared/input/posts.csv',
  header 'true',
  inferSchema 'true'
);

CREATE OR REPLACE TEMPORARY VIEW users
USING csv OPTIONS (
  path '/opt/spark/work-dir/shared/input/users.csv',
  header 'true',
  inferSchema 'true'
);

SELECT * FROM posts LIMIT 5;

# In spark-sql, Task 2 verficiation 
SELECT
    u.age_group AS age_group,
    COUNT(*) AS posts,
    ROUND(AVG(p.likes), 2) AS avg_likes,
    ROUND(AVG(p.retweets), 2) AS avg_retweets
FROM posts p
JOIN users u ON p.user_id = u.user_id
GROUP BY u.age_group
ORDER BY avg_likes DESC;

# In spark-sql, same for Task 3
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
ORDER BY sentiment;

# Exit 
QUIT;

# starting the sqp for dataframe 
docker exec -it spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077
```
In the PySpark shell, the CSV files were loaded into DataFrames, registered as temporary views, and Task 2 was executed using the DataFrame API. Its output matched the SQL output.

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```
===== task1 =====
#mood      1604
#tech      1582
#darkmode  1040
#love      1019
#ux        1006
#update    1003
#ai        696
#cleanui   670
#design    365
#bug       365
```
The most frequent normalized hashtag was #mood, with 1,604 occurrences. The implementation splits the hashtag field into individual values, converts them to lowercase, trims surrounding whitespace, and counts the normalized values.

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?
 - 2 distinct original spellings: #MOOD and #mood.


### Task 2: engagement by age group (Spark SQL)

```
age_group  posts  avg_likes  avg_retweets
Senior     1977   55.63      13.88
Teen       1865   51.38      13.36
Adult      1158   47.77      11.91

Senior users had the highest average likes (55.63) and average retweets (13.88). Teen users were second in both averages, while adults had the lowest averages in this dataset.
```

### Task 3: sentiment versus engagement (Spark SQL)

```
sentiment  posts  avg_likes  avg_retweets
Negative   1122   25.04      6.55
Neutral    2222   47.89      11.93
Positive   1656   76.45      19.50
```

Does sentiment pay on your data? Say it with the numbers.
 - Yes, positive sentiment was associated with higher engagement in this dataset. Positive posts averaged 76.45 likes and 19.50 retweets, compared with 25.04 likes and 6.55 retweets for negative posts. Neutral posts were in between, averaging 47.89 likes and 11.93 retweets. These are observed associations, not proof that sentiment causes engagement.


### Task 4: top verified users (DataFrame API)

```
username       total_likes  total_retweets  reach
@daily_vibes49 17700        4405            22105
@calm_mind22   17290        4167            21457
@calm_mind19   16010        4529            20539
@calm_mind3    15466        4424            19890
@designer39    8674         2101            10775

The highest-ranked verified user was @daily_vibes49, with a reach of 22,105. Reach was calculated as total likes plus total retweets, and the output contains the top five verified users sorted by descending reach.
```

---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

```
=== task2: physical plan of the SQL version ===
26/10/23 23:36:09 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 23:36:09 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#1))
26/10/23 23:36:09 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 23:36:09 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#8))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#54 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#54 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>


Task 2 DataFrame explain(True) output

=== task2: physical plan of the DataFrame version ===
26/10/23 23:36:09 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 23:36:09 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#1))
26/10/23 23:36:09 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 23:36:09 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#8))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#134 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#134 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>



```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?

The SQL and DataFrame implementations returned the same three age-group rows and the same averages. However, the two physical plans should be compared using their actual output; they are not guaranteed to be textually identical.

In the DataFrame plan, BroadcastHashJoin is the join operator. Spark chose an inner broadcast hash join and broadcast the users-side data (BuildRight) to join records by user_id.

There are two visible Exchange operators:

Exchange hashpartitioning(age_group, 200) redistributes intermediate records by age group so that records belonging to the same group can be aggregated together.

Exchange rangepartitioning(avg_likes DESC NULLS LAST, 200) prepares the results for the final ordered output.

The HashAggregate operations show partial aggregation followed by final aggregation. This lets Spark combine intermediate counts and averages rather than sending every raw record directly through the final aggregation stage.

SQL and the DataFrame API are alternative ways to express the same data transformation. The results here show that either approach can produce the same answer. The execution plan, rather than the source syntax alone, explains how Spark chose to execute the query.

---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

- createOrReplaceTempView("posts") registered the existing posts DataFrame under the SQL name posts in the current Spark session. It allowed queries such as SELECT * FROM posts LIMIT 5 and the Task 2 and Task 3 SQL queries to refer to that DataFrame using SQL syntax.

- It did not create a permanent database table or persist a separate copy of the data. The view is a temporary name for the DataFrame's query plan; the CSV data remains in its input files, and Spark reads it as needed.

- The temporary view is scoped to the Spark session and is no longer available after that session ends. To keep data available in later sessions, the data can be written to persistent storage, such as Parquet files, or saved as a table in a persistent catalog.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.


- During development, Task 1 initially failed because explode() was nested inside other expressions in the same projection.

The error was: AnalysisException: [UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS] 

The issue was resolved by separating the transformation into two steps:
1. Split and explode the hashtag field into individual rows.
2. Apply lower() and trim() to the resulting hashtag column, then group and count.
After this change, Task 1 executed successfully. The complete program subsequently ran all four tasks without errors. 

- The interactive PySpark shell also printed this warning:
        - WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

This warning did not prevent Spark from running the queries or producing the expected results.
