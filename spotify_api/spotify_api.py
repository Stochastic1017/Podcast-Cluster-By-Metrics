import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import itertools
import sqlite3
import logging
from tqdm import tqdm
import concurrent.futures
import backoff
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("spotify_scraper.log"),
        logging.StreamHandler()
    ]
)

def debug_print(message):
    """Utility for printing and logging simultaneously."""
    print(message)
    logging.info(message)

# Spotify API client
def get_spotify_client():
    """Create a Spotify client."""
    debug_print("Initializing Spotify client...")
    return spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        ),
        requests_timeout=30
    )

# Database manager for podcast storage
class DatabaseManager:
    def __init__(self, db_name="podcasts.db"):
        """Initialize the database manager and create necessary tables."""
        debug_print(f"Initializing database manager with DB: {db_name}")
        self.db_name = db_name
        self.init_db()

    def init_db(self):
        """Create tables for storing podcasts and tracking query progress."""
        debug_print("Creating database tables if not exist...")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS podcasts (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    description TEXT,
                    publisher TEXT,
                    total_episodes INTEGER,
                    explicit BOOLEAN,
                    media_type TEXT,
                    available_markets TEXT,
                    languages TEXT,
                    image_url TEXT,
                    external_url TEXT,
                    href TEXT,
                    recorded_countries TEXT,
                    market TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_progress (
                    query TEXT PRIMARY KEY,
                    completed BOOLEAN
                )
            ''')
            conn.commit()

    def save_podcast(self, podcast_data):
        """Save a podcast record to the database."""
        debug_print(f"Saving podcast: {podcast_data.get('name')}, ID: {podcast_data.get('id')}")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO podcasts 
                (id, name, description, publisher, total_episodes, explicit, media_type, available_markets, 
                 languages, image_url, external_url, href, recorded_countries, market)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                podcast_data.get("id"),
                podcast_data.get("name"),
                podcast_data.get("description"),
                podcast_data.get("publisher"),
                podcast_data.get("total_episodes"),
                podcast_data.get("explicit"),
                podcast_data.get("media_type"),
                ", ".join(podcast_data.get("available_markets", [])) if podcast_data.get("available_markets") else None,
                ", ".join(podcast_data.get("languages", [])) if podcast_data.get("languages") else None,
                podcast_data.get("images", [{}])[0].get("url"),
                podcast_data.get("external_urls", {}).get("spotify"),
                podcast_data.get("href"),
                ", ".join(podcast_data.get("available_markets", [])) if podcast_data.get("available_markets") else None,
                podcast_data.get("market", "US")
            ))
            conn.commit()

    def is_query_completed(self, query):
        """Check if a query has already been processed."""
        debug_print(f"Checking if query '{query}' is completed...")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT completed FROM query_progress WHERE query = ?', (query,))
            result = cursor.fetchone()
            completed = bool(result and result[0])
            debug_print(f"Query '{query}' completed: {completed}")
            return completed

    def mark_query_completed(self, query):
        """Mark a query as completed in the database."""
        debug_print(f"Marking query '{query}' as completed.")
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO query_progress (query, completed)
                VALUES (?, ?)
            ''', (query, True))
            conn.commit()

# Generate all possible three-character prefixes
def generate_prefixes():
    """
    Generate all combinations of three lowercase letters from 'aaa' to 'zzz'.

    Returns:
        list: A list of all three-character prefixes.
    """
    debug_print("Generating all three-character prefixes...")
    prefixes = ["".join(p) for p in itertools.product("abcdefghijklmnopqrstuvwxyz", repeat=3)]
    debug_print(f"Generated {len(prefixes)} prefixes.")
    return prefixes

# Retry Spotify API calls with exponential backoff
@backoff.on_exception(
    backoff.expo,
    (spotipy.exceptions.SpotifyException, Exception),
    max_tries=5,
)
def fetch_data(spotify_client, query, offset, limit=50):
    debug_print(f"Fetching data for query '{query}' at offset {offset}...")
    return spotify_client.search(q=query, type="show", market="US", limit=limit, offset=offset)

# Process a single query
def process_query(query, spotify_client, db_manager):
    total_podcasts = 0
    offset = 0
    limit = 50

    debug_print(f"Starting processing for query '{query}'...")
    while offset < 1000:  # Spotify's API limit for pagination
        try:
            results = fetch_data(spotify_client, query, offset, limit)
            shows = results.get("shows", {}).get("items", [])
            if not shows:
                debug_print(f"No shows found for query '{query}' at offset {offset}.")
                break

            for show in shows:
                podcast_data = {
                    "id": show.get("id"),
                    "name": show.get("name"),
                    "description": show.get("description"),
                    "publisher": show.get("publisher"),
                    "total_episodes": show.get("total_episodes"),
                    "explicit": show.get("explicit"),
                    "media_type": show.get("media_type"),
                    "available_markets": show.get("available_markets"),
                    "languages": show.get("languages"),
                    "images": show.get("images"),
                    "external_urls": show.get("external_urls"),
                    "href": show.get("href"),
                    "market": "US",  # Default market
                }
                db_manager.save_podcast(podcast_data)
                total_podcasts += 1

            offset += limit
            debug_print(f"Processed offset {offset} for query '{query}'.")

        except Exception as e:
            debug_print(f"Error fetching query '{query}' at offset {offset}: {e}")
            logging.error(f"Error fetching query '{query}' at offset {offset}: {e}")
            break

    db_manager.mark_query_completed(query)
    debug_print(f"Finished processing query '{query}'. Total podcasts: {total_podcasts}")
    return total_podcasts

# Main function to process all queries
def main():
    debug_print("Starting main function...")
    db_manager = DatabaseManager()
    spotify_client = get_spotify_client()

    # Generate all three-character prefixes
    prefixes = generate_prefixes()

    # Create queries for all prefixes
    queries = [f"{prefix}" for prefix in prefixes]
    debug_print(f"Generated {len(queries)} queries.")

    # Process queries in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(tqdm(
            executor.map(lambda q: process_query(q, spotify_client, db_manager), queries),
            total=len(queries),
            desc="Processing Queries"
        ))

    total_scraped = sum(results)
    debug_print(f"Total podcasts scraped: {total_scraped}")
    logging.info(f"Total podcasts scraped: {total_scraped}")

if __name__ == "__main__":
    main()
