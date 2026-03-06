# Spotify End-to-End ETL Pipeline | AWS

An end-to-end data pipeline that extracts data from the Spotify API, transforms it using AWS Glue and PySpark, and loads it into S3 for analytics.

---

## Architecture

```
Spotify API
    |
    v
AWS Lambda (Extract)
    |
    v
Amazon S3 (Raw Data)
    |
    v
AWS Lambda / AWS Glue (Transform)
    |
    v
Amazon S3 (Transformed Data)
    |
    v
AWS Glue Crawler → AWS Athena (Analytics)
```

---

## Tech Stack

- **Cloud:** AWS (Lambda, S3, Glue, Athena, CloudWatch)
- **Languages:** Python, PySpark
- **Libraries:** Spotipy, Boto3, Pandas
- **Trigger:** CloudWatch EventBridge (daily schedule)

---

## Pipeline Overview

### 1. Extraction
- A Lambda function (`spotify_api_data_extract.py`) is triggered daily via CloudWatch
- Connects to the Spotify API using `spotipy` and pulls tracks from the Global Top 50 playlist
- Raw JSON response is stored in `s3://spotify-etl-project-nitish/raw_data/to_processed/`

### 2. Transformation
Two transformation approaches are implemented:

**Pandas (Lambda)** — `spotify_transformation_load_function.py`
- Reads raw JSON from S3
- Extracts and normalizes three entities: Albums, Artists, Songs
- Adds `explicit` field to capture content rating per track
- Writes cleaned CSVs back to S3 under `transformed_data/`
- Moves processed files to `raw_data/processed/` to avoid reprocessing

**PySpark (AWS Glue)** — `spotify_transformation.py`
- Same transformation logic implemented in PySpark for scalability
- Uses AWS Glue DynamicFrames to read/write from S3
- Handles nested JSON structures using `explode()` and `col()` functions

### 3. Load & Analytics
- AWS Glue Crawler scans transformed CSVs and updates the Glue Data Catalog
- Data is queryable via AWS Athena using standard SQL

---

## Data Model

**Songs Table**
| Column | Type | Description |
|---|---|---|
| song_id | string | Unique Spotify track ID |
| song_name | string | Track name |
| duration_ms | int | Track duration in milliseconds |
| popularity | int | Spotify popularity score (0-100) |
| explicit | boolean | Whether track has explicit content |
| song_added | date | Date added to playlist |
| album_id | string | Foreign key to Albums |
| artist_id | string | Foreign key to Artists |

**Albums Table**
| Column | Type | Description |
|---|---|---|
| album_id | string | Unique Spotify album ID |
| name | string | Album name |
| release_date | date | Album release date |
| total_tracks | int | Number of tracks in album |
| url | string | Spotify URL |

**Artists Table**
| Column | Type | Description |
|---|---|---|
| artist_id | string | Unique Spotify artist ID |
| artist_name | string | Artist name |
| external_url | string | Spotify artist URL |

---

## Key Implementation Details

- Logging added across all Lambda and Glue functions via Python's `logging` module for CloudWatch observability
- Duplicate records are dropped on `album_id`, `artist_id`, and `song_id` to ensure data integrity
- Processed raw files are automatically moved from `to_processed/` to `processed/` after each run
- Both Pandas and PySpark transformation scripts are provided — Pandas for lightweight runs, Glue/Spark for scale

---

## Project Structure

```
spotify-pipeline/
│
├── spotify_api_data_extract.py               # Lambda: Spotify API → S3 raw
├── (python) spotify_transformation_load_function.py  # Lambda: Transform + Load using Pandas
├── (spark) spotify_transformation.py         # AWS Glue: Transform + Load using PySpark
└── README.md
```