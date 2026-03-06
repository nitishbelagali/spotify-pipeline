import json
import os
import logging
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    logger.info("Initializing Spotify client credentials")
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    logger.info(f"Fetching tracks for playlist URI: {playlist_URI}")
    
    spotify_data = sp.playlist_tracks(playlist_URI)
    logger.info(f"Successfully fetched {len(spotify_data.get('items', []))} tracks from Spotify API")
    
    client = boto3.client('s3')
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    bucket_name = "spotify-etl-project-nitish"
    
    logger.info(f"Uploading raw data to S3 bucket: {bucket_name}, file: {filename}")
    client.put_object(
        Bucket=bucket_name,
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
    )
    logger.info("Raw data successfully uploaded to S3")