import requests
import json
import random
import time
import os

# OMDb API configuration
API_KEY = "Your API KEY"  # replace with your API key
BASE_URL = "API URL" #replace with your API URL

# List of genres for random selection
genres = ["Action", "Comedy", "Drama", "Thriller", "Romance", "Sci-Fi", "Horror", 
          "Adventure", "Fantasy", "Crime", "Mystery", "Biography", "War"]

def fetch_movie_data(imdb_id):
    """Fetches detailed movie data by IMDb ID."""
    try:
        response = requests.get(BASE_URL, params={"i": imdb_id, "apikey": API_KEY})
        data = response.json()
        return data if data.get('Response') == 'True' else None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for IMDb ID {imdb_id}: {e}")
        return None

def fetch_movies_random_genre():
    """Fetches movies of a random genre and saves them as a JSON file."""
    genre = random.choice(genres)
    movies = []
    page = 1

    print(f"Fetching movies of genre '{genre}'...")

    # Set the directory to save the file
    data_dir = "/workspaces/Data-Engineering-Pipeline/Data"
    
    while len(movies) < 50:
        try:
            response = requests.get(BASE_URL, params={"s": genre, "apikey": API_KEY, "type": "movie", "page": page})
            data = response.json()

            if data.get('Response') == 'True':
                for movie in data['Search']:
                    movie_data = fetch_movie_data(movie['imdbID'])
                    if movie_data:
                        movies.append(movie_data)
                        if len(movies) >= 15:
                            break
            else:
                print(f"Error: {data.get('Error', 'Unknown error')}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

        page += 1
        time.sleep(1)

    # Create directory only if it doesn't exist
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Save the movie data to the specified directory
    with open(f"{data_dir}/{genre}_movies.json", "w") as outfile:
        json.dump(movies, outfile, indent=4)

    print(f"Fetched {len(movies)} movies for genre '{genre}' and saved to {data_dir}/{genre}_movies.json")

# Run the data extraction
fetch_movies_random_genre()

