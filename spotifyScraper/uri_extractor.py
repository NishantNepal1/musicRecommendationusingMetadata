import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

with open('config.json') as config:
    cid = config['cid']
    secret = config['secret']


def uri_extractor():
    playlist_link = input("Paste your URL here")
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=cid,
                                                               client_secret=secret))
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"] for x in sp.playlist_tracks(playlist_URI)["items"]]

    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"] for x in sp.playlist_tracks(playlist_URI)["items"]]

    f = open('playlist.csv', 'x')
    f.write("Song Name\n")
    for track in sp.playlist_tracks(playlist_URI)["items"]:
        track_pop = track["track"]["popularity"]
        track_name = track['track']["name"]
        print(type(track_name))
        f.write('{uri}\n'.format(
            uri=track['track']['uri'],
            # track_name=track["track"]["name"],
            # artist_name=track["track"]["artists"][0]["name"]
        ))
    f.close()
    return "playlist.csv"


if __name__ == '__main__':
    uri_extractor()
