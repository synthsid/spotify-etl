## Getting Started
   
##  ETL Flow Overview
This project builds a scalable ETL (Extract, Transform, Load) pipeline using the Spotify Web API, Docker, Apache Airflow, and PostgreSQL. 
The pipeline extracts artist, album, and track data, transforms it into a dimensional model, and loads it into a Postgres-based data warehouse(I am simulating Redshift by doing everything in Postgres) for downstream analytics.

1. **Extract**  
   Use Spotify API to pull metadata about artists, albums, tracks, etc. 
   
2. **Transform**  
   Clean, normalize, and structure the data into a star schema  with fact and dimension tables.

3. **Load**  
   Load data into a PostgreSQL database via Python and Airflow.

4. **Orchestrate**  
   DAGs manage scheduling, retries, and task dependencies for hourly or daily execution.

##  Tech Stack

- **Python** – Core ETL scripting
- **Docker & Docker Compose** – Containerized environment
- **PostgreSQL** – Data warehouse storage
- **Apache Airflow** – Workflow orchestration
- **Spotify Web API** – Data source
- **dotenv** – Secure API authentication

##  Project Structure
spotify-etl/
dags/ # Airflow DAGs for orchestration

scripts/ # Various Python and SQL scripts for ETL, DDL and other

postgres-config/ # PostgreSQL initialization and config

Dockerfile # Airflow + Python setup

docker-compose.yml # Compose services: Airflow, Postgres, etc.

postgres-init-multiple-db.sh # Init script for PostgreSQL

wait-for-postgres.sh # Ensures Postgres is ready before DAGs run

test_connection.py # Connection test to validate setup

requirements.txt # Python dependencies


### Prerequisites

- Docker + Docker Compose installed
- Spotify API credentials (Client ID + Client Secret)

### Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/synthsid/spotify-etl.git
   cd spotify-etl
   
2. Create a .env file:

   
Spotify API credentials

CLIENT_ID= your_spotify_client_id

CLIENT_SECRET= your_spotify_client_secret

Airflow admin & webserver

this will be used in the 4th step so create your own user/pass

AIRFLOW__WEBSERVER__SECRET_KEY= "create your own"

AIRFLOW_ADMIN_USERNAME= 

AIRFLOW_ADMIN_PASSWORD=



Airflow metadata database 

take a look at docker-compose.yml and see how these credentials are used

AIRFLOW_DB=

AIRFLOW_DB_USER=

AIRFLOW_DB_PASS=



Spotify ETL data warehouse
SPOTIFY_DB=

SPOTIFY_DB_USER=

SPOTIFY_DB_PASS=


Postgres Host/Docker Ports
I personally ran into some issues so I had to use 2 different ports

DB_CONTAINER_PORT=       # used by Docker (Airflow)

DB_HOST_PORT=            # used by host (VS Code / local scripts)

DB_HOST=

DB_PORT=${DB_CONTAINER_PORT}

For local dev (used in test_connection.py, not Docker)

LOCAL_DB_HOST=

LOCAL_DB_PORT=${DB_HOST_PORT}

3. Start the containers:
docker-compose up --build

4. Access Airflow(use any browser) at:
http://localhost:8080



## Sample DAGs

spotify_etl.py – Main ETL process

agg_total_tracks_by_artist.py – Aggregates total tracks per artist

agg_album_stats.py – Calculates album-level stats

agg_artist_popularity.py – Tracks popularity changes over time
