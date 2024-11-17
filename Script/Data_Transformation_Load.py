from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, first
import os
import glob
from pyspark.sql.functions import col, to_date, regexp_replace
from pyspark.sql.types import IntegerType, FloatType, LongType, DateType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("MoviesDataTransformation").getOrCreate()

data_dir = "/workspaces/Data-Engineering-Pipeline/data"  # Ensure this directory exists and has JSON files

def get_latest_json_file(directory):
    json_files = glob.glob(os.path.join(directory, "*_movies.json"))
    if not json_files:
        return None  # Return None if no JSON files are found
    latest_file = max(json_files, key=os.path.getctime)
    return latest_file

# Load the most recent data file
json_file_path = get_latest_json_file(data_dir)
if json_file_path:
    print(f"Loading data from: {json_file_path}")

    # Load JSON file into a PySpark DataFrame
    df = spark.read.json(json_file_path, multiLine=True)

    # Drop unnecessary columns
    df = df.drop("Poster", "Production", "DVD", "Website")

    # Explode and pivot the Ratings array
    df_exploded = df.select("imdbID", "Title", "Genre", explode(col("Ratings")).alias("rating"))
    df_ratings = df_exploded.select("imdbID", col("rating.Source").alias("Source"), col("rating.Value").alias("Value"))
    df_pivot = df_ratings.groupBy("imdbID").pivot("Source").agg(first("Value"))
    df_combined = df.join(df_pivot, on="imdbID").drop("Ratings")

    # Reorder columns and select final schema
    final_columns = [
        "imdbID", "Type", "Title", "Plot", "Released", "Runtime", "BoxOffice", "Rated", "Genre",
        "Actors", "Director", "Writer", "Country", "Language", "Awards", "Metascore",
        "imdbRating", "imdbVotes", "Internet Movie Database", "Rotten Tomatoes",
        "Metacritic", "Response", "Year"
    ]
    
    df_final = df_combined.select(*final_columns)

else:
    print("No JSON files found in the specified directory.")

df_Final = df_final \
.withColumn("Released", to_date(col("Released"), "MMM d, yyyy")) \
.withColumn("Year", col("Year").cast(IntegerType())) \
.withColumn("Metascore", col("Metascore").cast(IntegerType())) \
.withColumn("imdbRating", col("imdbRating").cast(FloatType())) \
.withColumn("Internet Movie Database", col("Internet Movie Database").cast(FloatType())) \
.withColumn("Rotten Tomatoes", regexp_replace(col("Rotten Tomatoes"), "%", "").cast(FloatType()) / 100) \
.withColumn("Metacritic", col("Metacritic").cast(FloatType())) \
.withColumn("imdbVotes", regexp_replace(col("imdbVotes"), ",", "").cast(LongType())) \
.withColumn("BoxOffice", regexp_replace(col("BoxOffice"), "[$,]", "").cast(LongType()))


# Loading data to SQL Table

db_url = "jdbc:mysql://localhost:3306/DataEngg"  # Replace with your database URL
db_properties = {
    "user": "root",  # Replace with your database username
    "password": "Bleach@8055",  # Replace with your database password
    "driver": "com.mysql.cj.jdbc.Driver"  # Use the appropriate JDBC driver
}

table_name = "Movies"

try:
    df_final.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .mode("overwrite") \
        .save()
    print(print(f"Data successfully loaded into table {table_name}"))

except Exception as e:
    print(f"Error occurred while loading data into the table: {str(e)}")





