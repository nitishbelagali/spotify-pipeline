import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://spotify-etl-project-nitish/raw_data/to_processed/"
logger.info(f"Reading raw data from S3 path: {s3_path}")

source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json"
)

spotify_df = source_dyf.toDF()
logger.info(f"Loaded {spotify_df.count()} records from S3")

def process_albums(df):
    logger.info("Processing albums data")
    df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(["album_id"])
    logger.info(f"Albums processed: {df.count()} unique records")
    return df

def process_artists(df):
    logger.info("Processing artists data")
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    df_artists_exploded = df_items_exploded.select(explode(col("item.track.artists")).alias("artist"))
    df_artists = df_artists_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("external_url")
    ).drop_duplicates(["artist_id"])
    logger.info(f"Artists processed: {df_artists.count()} unique records")
    return df_artists

def process_songs(df):
    logger.info("Processing songs data")
    df_exploded = df.select(explode(col("items")).alias("item"))
    df_songs = df_exploded.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("song_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.track.popularity").alias("popularity"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id"),
        col("item.track.artists")[0]["id"].alias("artist_id"),
        col("item.track.explicit").alias("explicit")  # Added: captures whether track has explicit content
    ).drop_duplicates(["song_id"])
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    logger.info(f"Songs processed: {df_songs.count()} unique records")
    return df_songs

# Process data
album_df = process_albums(spotify_df)
artist_df = process_artists(spotify_df)
song_df = process_songs(spotify_df)

def write_to_s3(df, path_suffix, format_type="csv"):
    full_path = f"s3://spotify-etl-project-nitish/transformed_data/{path_suffix}/"
    logger.info(f"Writing data to S3: {full_path}")
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": full_path},
        format=format_type
    )
    logger.info(f"Successfully written to {full_path}")

# Write data to S3
write_to_s3(album_df, "album/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(artist_df, "artist/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(song_df, "songs/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")

logger.info("All data successfully written to S3. Committing Glue job.")
job.commit()