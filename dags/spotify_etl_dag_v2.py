# import python scripts like artist and albums
from utils.common_utils import get_connection, get_token

def run_spotify_etl():
    con = get_connection
    token = get_token()

    artist_names = [ "Drake", "Taylor Swift", "Kendrick Lamar", "Adele", "Bad Bunny", 
                    "Billie Eilish", "SZA", "Travis Scott", "Dua Lipa", "Post Malone"]
    
    artist_ids = load_artists(conn, token, artist_names)
    # album_ids = load_albums(conn, token, artist_ids)

    #TODO: now we use the artist_ids here to pass it to albums or other modules based on what the API needs

    conn.close()