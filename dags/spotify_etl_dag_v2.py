# import python scripts like artist and albums
from utils.common_utils import get_connection, get_token
from extract.artists import load_artists
from extract.albums import load_albums

def run_spotify_etl():
    con = get_connection
    token = get_token()

    artist_names = [ "Drake", "Taylor Swift", "Kendrick Lamar", "Adele", "Bad Bunny", 
                    "Billie Eilish", "SZA", "Travis Scott", "Dua Lipa", "Post Malone"]
    
    artist_ids = load_artists(conn, token, artist_names)
    album_ids = load_albums(conn, token, artist_ids)

    #TODO: use album_ids to pass it to tracks module next

    conn.close()