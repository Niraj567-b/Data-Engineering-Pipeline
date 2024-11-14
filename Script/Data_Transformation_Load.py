from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, first
import os
import glob
import psycopg2

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
        "Metacritic", "Response", "Year", "Website"
    ]
    df_final = df_combined.select(*final_columns)
    
else:
    print("No JSON files found in the specified directory.")
 
# loading data Amazon Redshift DB

# Connect to your Redshift database
conn = psycopg2.connect(
    dbname="dev", 
    user="admin", 
    password="HTSNXefbhu141+", 
    host="moviedata.182399678016.ap-south-1.redshift-serverless.amazonaws.com:5439/dev", 
    port="5439"
)
cur = conn.cursor()

# Iterate over the DataFrame rows and insert into Redshift
for row in df_final.collect():
    cur.execute("""
        INSERT INTO movies (imdbID, Title, Genre, Plot, Released, Runtime, BoxOffice, Rated, Year, Actors, Director, Writer, Country, Language, Awards, Metascore, imdbRating, imdbVotes, InternetMovieDatabase, RottenTomatoes, Metacritic, Response)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['imdbID'],
        row['Title'],
        row['Genre'],
        row['Plot'],
        row['Released'],
        row['Runtime'],
        row['BoxOffice'],
        row['Rated'],
        row['Year'],
        row['Actors'],
        row['Director'],
        row['Writer'],
        row['Country'],
        row['Language'],
        row['Awards'],
        row['Metascore'],
        row['imdbRating'],
        row['imdbVotes'],
        row['InternetMovieDatabase'],
        row['RottenTomatoes'],
        row['Metacritic'],
        row['Response']
    ))

# Commit and close the connection
conn.commit()
cur.close()
conn.close()
