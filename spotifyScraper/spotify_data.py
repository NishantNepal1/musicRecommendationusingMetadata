import json

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

with open("config.json") as f:
    config = json.load(f)

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=config["client_id"],
                                                           client_secret=config["client_secret"]))

results = sp.search(q='weezer', limit=20)
for idx, track in enumerate(results['tracks']['items']):
    print(idx, track['name'])
