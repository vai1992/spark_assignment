# Importing main packages required for the project
from pyspark.sql.functions import udf ,lower ,col ,upper ,split ,count ,sum ,when ,desc
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

#Preparing Spark Session
SparkContext._ensure_initialized()
spark = SparkSession._create_shell_session()


#Reading Input from the Input File Provided
df = spark.read.option("header",True).csv('../input/IPL_SCHEDULE_2008_2020.csv')

#Filtering out all eliminators, qualifiers and final matches from the data
df = df.filter(lower(col('Match_Info')).contains('match'))

# Selecting only necessary fields from the data
df = df[['Match_id','IPL_year','Match_Team','Match_Result']]

#Filling null values with default value of 'null' string
df = df.fillna('null')

#Filtering out all the future planned matches ex- future IPL 2020 matches in the data provided
df = df.filter(col('Match_Result') != 'null')

#Reading Team Mappings with their city(keywords) from a created file. This gives the name of latest Team names corresponding to the city/word
team_mapping = spark.read.option("header",True).csv('../extra_input/Team Mapping.csv')


#Extracting Team 1 and Team 2 names from the Match_Team field using " vs " as splitter
df = df.withColumn('Team_1',upper(split(col("Match_Team"), " vs ").getItem(0)))
df = df.withColumn('Team_2',upper(split(col("Match_Team"), " vs ").getItem(1)))

#Also adding another column denoting a Tied Up match or not
df = df.withColumn('Tied',lower(col("Match_Result")).contains('tied'))

#Creating a iterable list for team mapping
iter = team_mapping.rdd.collect()

#Declaration of function to provide the winner from a given text field
def winner(info):
    if type(info) != str:
        return 'null'
    for rows in iter:
        if (info.lower()).find(rows.City.lower()) >= 0:
            return rows.Team
    return 'null'

#Defined a UDF to be used in getting a team name from text
func_udf = udf(winner, StringType())

#Extracted winner with the latest team name
df = df.withColumn('Winner', func_udf(df.Match_Result) )

#Updating the Team 1 and Team 2 with latest Team names
df = df.withColumn('Team_1', func_udf(df.Team_1) )
df = df.withColumn('Team_2', func_udf(df.Team_2) )


#Inserting fields denoting the Points of both the teams for the particular match
df = df.withColumn("Team_1_Points", F.when(df["Team_1"] == df["Winner"], 2).when(df["Team_2"] == df["Winner"], 0).when(df["Match_Result"] == 'null', 0).otherwise(1))
df = df.withColumn("Team_2_Points", F.when(df["Team_2"] == df["Winner"], 2).when(df["Team_1"] == df["Winner"], 0).when(df["Match_Result"] == 'null', 0).otherwise(1))

#Renaming column names and creating a new dataframe. This final df has data for each innings of the match.
final_df = df[['IPL_year','Team_1','Team_1_Points','Tied']].withColumnRenamed('Team_1', 'Team').withColumnRenamed('Team_1_Points', 'Points')
final_df = final_df.union(df[['IPL_year','Team_2','Team_2_Points','Tied']].withColumnRenamed('Team_2', 'Team').withColumnRenamed('Team_2_Points', 'Points'))

#Tagging each innings of a match with Won, Lost and Tied flags
final_df = final_df.withColumn("Won",F.when(final_df["Points"] == 2, 1).otherwise(0))
final_df = final_df.withColumn("Lost",F.when(final_df["Points"] == 0, 1).otherwise(0))
final_df = final_df.withColumn("Tied",F.when(final_df["Tied"], 1).otherwise(0))

#Aggregating the final df to extract the end result
final_df = final_df.groupBy("IPL_year", "Team").agg(count("*").alias("Matches Played") , sum("Won").alias("Matches_Won") , sum("Lost").alias("Matches_Lost") , sum("Tied").alias("Matches_Tied") , sum("Points").alias("Overall_Points")  ).orderBy('IPL_year',desc('Overall_Points'),desc("Matches_Won"))

#Writing, Storing the end result in an output file
final_df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv').save('../output',header = 'true')