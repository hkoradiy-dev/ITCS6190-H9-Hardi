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
In the interactive Spark SQL shell, temporary views were created for posts and users, then the Task 2 SQL and Task 3 SQL queries were run. In the PySpark shell, the Task 2 DataFrame query was run and its plan was inspected using result.explain(True). The complete main.py run completed all four tasks without errors.

README deviations: The interactive Spark SQL and PySpark shells were used in addition to running the complete program with spark-submit, to verify the SQL and DataFrame queries and inspect the execution plan.
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

- Apart from generated expression IDs (#n) and plan_id values, the two plans are identical. This shows that Spark SQL and the DataFrame API go through the same Spark optimizer and are optimized to the same execution plan for this query. Therefore, there is no performance difference between the SQL and DataFrame versions of this query simply because one is written in SQL and the other uses the DataFrame API. Both express the same computation and Spark executes the same optimized plan.

- The join operator is BroadcastHashJoin, an inner join on user_id. Spark chose a broadcast hash join because the users table is small enough to broadcast. Spark broadcasts the users-side data to the workers processing the posts, so the posts rows do not need to be shuffled across the cluster for the join itself.

The physical plan contains two Exchange operators:
- Exchange hashpartitioning(age_group, 200) redistributes records by age_group so rows for each group can be combined correctly during aggregation.
- Exchange rangepartitioning(avg_likes DESC NULLS LAST, 200) prepares the result for the requested sort by descending average likes.
The plan also contains partial and final HashAggregate stages, allowing Spark to combine intermediate counts and averages before producing final results. The important conclusion is that choosing SQL versus the DataFrame API does not by itself make this query faster: Spark optimizes both forms to the same plan.


---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

- createOrReplaceTempView("posts") registered the existing posts DataFrame under the SQL name posts in the current Spark session. This allowed SQL queries to refer to the DataFrame using FROM posts.

- It did not create a permanent database table or copy all the CSV data into a new table. It registered a temporary view of the DataFrame's query plan; the source CSV data remains in the input files and is read by Spark as needed.

- The temporary view is scoped to the Spark session and disappears when that session ends. To keep data available in a later session, write it to persistent storage (for example, Parquet files) or save it as a table in a persistent catalog.
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
