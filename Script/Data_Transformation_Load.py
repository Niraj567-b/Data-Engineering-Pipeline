from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, first
import os
import glob
import pyodbc
from pyspark.sql.functions import col, to_date, regexp_replace
from pyspark.sql.types import IntegerType, FloatType, LongType, DateType, StringType

# Initialize Spark session
# For PySpark
spark = SparkSession.builder \
    .appName("MoviesDataTransformation") \
    .getOrCreate()


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

# Convert the columns of df_Final to match the SQL Server schema
df_Final = df_final \
    .withColumn("Released", to_date(col("Released"), "yyyy-MM-dd")) \
    .withColumn("Year", col("Year").cast(IntegerType())) \
    .withColumn("Metascore", col("Metascore").cast(IntegerType())) \
    .withColumn("imdbRating", col("imdbRating").cast(FloatType())) \
    .withColumn("Internet Movie Database", col("Internet Movie Database").cast(FloatType())) \
    .withColumn("Rotten Tomatoes", col("Rotten Tomatoes").cast(FloatType())) \
    .withColumn("Metacritic", col("Metacritic").cast(FloatType())) \
    .withColumn("imdbVotes", regexp_replace(col("imdbVotes"), ",", "").cast(LongType())) \
    .withColumn("BoxOffice", regexp_replace(col("BoxOffice"), "[$,]", "").cast(LongType())) \
    .withColumn("Type", col("Type").cast(StringType())) \
    .withColumn("Title", col("Title").cast(StringType())) \
    .withColumn("Plot", col("Plot").cast(StringType())) \
    .withColumn("Runtime", col("Runtime").cast(StringType())) \
    .withColumn("Rated", col("Rated").cast(StringType())) \
    .withColumn("Genre", col("Genre").cast(StringType())) \
    .withColumn("Actors", col("Actors").cast(StringType())) \
    .withColumn("Director", col("Director").cast(StringType())) \
    .withColumn("Writer", col("Writer").cast(StringType())) \
    .withColumn("Country", col("Country").cast(StringType())) \
    .withColumn("Language", col("Language").cast(StringType())) \
    .withColumn("Awards", col("Awards").cast(StringType())) \
    .withColumn("Response", col("Response").cast(StringType()))

# Azure SQL Database credentials and configuration
server = 'niraj-b.database.windows.net'  # Azure SQL Server name
database = 'DataEngg'  # Database name
username = 'Niraj'  # Database username
password = 'Bleach@8055'  # Database password
driver = '{ODBC Driver 17 for SQL Server}'  # ODBC driver for SQL Server

# Connection string for Azure SQL Database
connection_string = f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"

# Establish the connection
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define your insert query (adjust column names to match your table)
insert_query = """
        INSERT INTO [dbo].[Movies] (
            [imdbID], [Type], [Title], [Plot], [Released], [Runtime], [BoxOffice], 
            [Rated], [Genre], [Actors], [Director], [Writer], [Country], [Language], 
            [Awards], [Metascore], [imdbRating], [imdbVotes], [Internet_Movie_Database], 
            [Rotten_Tomatoes], [Metacritic], [Response], [Year]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

def handle_null_values(row):
    # Ensure imdbID is always present and not None
    imdbID = row['imdbID'] if row['imdbID'] else 'Unknown'  # Default value if imdbID is missing

    return (
        imdbID,  # Ensure imdbID is always populated
        row['Type'] if row['Type'] else None,
        row['Title'] if row['Title'] else None,
        row['Plot'] if row['Plot'] else None,
        row['Released'] if row['Released'] else None,
        row['Runtime'] if row['Runtime'] else None,
        row['BoxOffice'] if row['BoxOffice'] else None,
        row['Rated'] if row['Rated'] else None,
        row['Genre'] if row['Genre'] else None,
        row['Actors'] if row['Actors'] else None,
        row['Director'] if row['Director'] else None,
        row['Writer'] if row['Writer'] else None,
        row['Country'] if row['Country'] else None,
        row['Language'] if row['Language'] else None,
        row['Awards'] if row['Awards'] else None,
        row['Metascore'] if row['Metascore'] else None,
        row['imdbRating'] if row['imdbRating'] else None,
        row['imdbVotes'] if row['imdbVotes'] else None,
        row['Internet Movie Database'] if row['Internet Movie Database'] else None,
        row['Rotten Tomatoes'] if row['Rotten Tomatoes'] else None,
        row['Metacritic'] if row['Metacritic'] else None,
        row['Response'] if row['Response'] else None,
        row['Year'] if row['Year'] else None  # Ensure this is not NULL if not applicable
    )

# Example function to insert data into Movies table

def insert_data(df):
    # Loop through the dataframe rows
    for row in df.collect():
        # Handle NULL values by replacing missing values with None
        data = handle_null_values(row)
        
        # Execute the insert query with the correct number of parameters
        cursor.execute(insert_query, data)
        conn.commit()  # Commit after each insertion (you can also batch commits for performance)

    print(f"Successfully inserted {df.count()} records into the [dbo].[Movies] table.")

# Assuming df_final is your final PySpark DataFrame after transformations
# Example PySpark DataFrame (df_final) should be available at this point

insert_data(df_Final)

# Close the connection
cursor.close()
conn.close()