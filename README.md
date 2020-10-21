# Spark Assignment

This is an assignment written in pyspark.

## Problem Statement

Generate year wise points-table for IPL using Spark.

## Rules

The following are the rules to be used for extracting the points table:

1. Points table should contain year, team name, no. of matches played, no. of matches
won, no. of matches lost, no. of matches tied, overall points ordered by year, points
2. Points Calculation:
3. Winning team gets 2 points
4. In case of tied match where there is no super over both teams get 1 point each
5. In case of tied match with super over, winning team 2 points
    Points Table:
    - In case of equal points, team with more wins is higher in the order
    - In case of equal points and equal wins, order doesn’t necessarily match
6. Team names have changed over years, while generating points table use the latest
names of the teams. Eg: Deccan Charges is changed to SunRisers Hyderabad, Delhi
Dare Devils is changed to Delhi Capitals.

## Folder Structure Design of the Solution

    ├── extra_input
    |   └── Team Mapping.csv                      # Extra Input provided used in the solution for Team Mapped to city Keywords
    ├── input
    |   └── IPL_SCHEDULE_2008_2020.csv            # Raw Input File for which Points table needs to be Presented
    ├── output
    |   └── part-00000-f78d7e1b-c98e-4058-9735-1f56827a784b-c000.csv       # End Result
    ├── project
    |   └── total_score.py                        # Main Python to process the input file and give output of Points Table


## How to run the program

Run the following command
```
$SPARK_HOME/bin/spark-submit project/total_score.py
```

## About the Main File

The main steps or transformation that were performed for the problem statement are as follows:

1. Reading Input from the Input File Provided
```
df = spark.read.option("header",True).csv('../input/IPL_SCHEDULE_2008_2020.csv')
```

2. Filtering out all eliminators, qualifiers and final matches from the data
```
df = df.filter(lower(col('Match_Info')).contains('match'))
```

3. Filtering out all the future planned matches Example - future IPL 2020 matches in the data provided that are never yet played
```
df = df.filter(col('Match_Result') != 'null')
```

4. Extracting Team 1 and Team 2 names from the Match_Team field using " vs " as splitter
```
df = df.withColumn('Team_1',upper(split(col("Match_Team"), " vs ").getItem(0)))
df = df.withColumn('Team_2',upper(split(col("Match_Team"), " vs ").getItem(1)))
```

5. Also adding another column denoting a Tied Up match or not
```
df = df.withColumn('Tied',lower(col("Match_Result")).contains('tied'))
```

6. Declaration of function to provide the winner from a given text field and Defined a UDF to be used in getting a team name from text
```
iter = team_mapping.rdd.collect()

def winner(info):
    if type(info) != str:
        return 'null'
    for rows in iter:
        if (info.lower()).find(rows.City.lower()) >= 0:
            return rows.Team
    return 'null'

func_udf = udf(winner, StringType())
```

7. Extracted winner with the latest team name using UDF. Here City Keywords are looked upon in Match Result Column and then corresponding Team Name is extracted using the Team Mapping
```
df = df.withColumn('Winner', func_udf(df.Match_Result) )
```

8. Updating the Team 1 and Team 2 with latest Team names. Using same logic for extracting Winner, Updated Team Names are updated as Team 1 and Team 2
```
df = df.withColumn('Team_1', func_udf(df.Team_1) )
df = df.withColumn('Team_2', func_udf(df.Team_2) )
```

9. Adding fields denoting the Points of both the teams for the particular match. Logic Used here is
    if Team_1 is Winner then Team_1 Point = 2 and Team_2 Point = 0,
    if Team_2 is Winner then Team_2 Point = 2 and Team_1 Point = 2,
    if No Result then both Team gets 1 points
```
df = df.withColumn("Team_1_Points", F.when(df["Team_1"] == df["Winner"], 2).when(df["Team_2"] == df["Winner"], 0).when(df["Match_Result"] == 'null', 0).otherwise(1))
df = df.withColumn("Team_2_Points", F.when(df["Team_2"] == df["Winner"], 2).when(df["Team_1"] == df["Winner"], 0).when(df["Match_Result"] == 'null', 0).otherwise(1))
```

10. Tagging each innings of a match with Won, Lost and Tied flags
```
final_df = final_df.withColumn("Won",F.when(final_df["Points"] == 2, 1).otherwise(0))
final_df = final_df.withColumn("Lost",F.when(final_df["Points"] == 0, 1).otherwise(0))
final_df = final_df.withColumn("Tied",F.when(final_df["Tied"], 1).otherwise(0))
```

11. Aggregating the final df to extract the end result
```
final_df = final_df.groupBy("IPL_year", "Team").agg(count("*").alias("Matches Played") , sum("Won").alias("Matches_Won") , sum("Lost").alias("Matches_Lost") , sum("Tied").alias("Matches_Tied") , sum("Points").alias("Overall_Points")  ).orderBy('IPL_year',desc('Overall_Points'),desc("Matches_Won"))
```

12. Writing, Storing the end result in an output file
```
final_df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv').save('../output',header = 'true')
```