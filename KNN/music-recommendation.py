import json
import psycopg2
import pandas as pd
import copy
import spotipy

from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import normalize
from operator import itemgetter
from tabulate import tabulate
from statistics import mean
from spotipy.oauth2 import SpotifyClientCredentials

# encode genre:
GENRE_ENCODING = {"Movie": 0,
                  "R&B": 1,
                  "A Capella": 2,
                  "Alternative": 3,
                  "Country": 4,
                  "Dance": 5,
                  "Electronic": 6,
                  "Anime": 7,
                  "Folk": 8,
                  "Blues": 9,
                  "Opera": 10,
                  "Hip-Hop": 11,
                  "Children Music": 12,
                  "Indie": 13,
                  "Pop": 14,
                  "Rap": 15,
                  "Classical": 16,
                  "Reggae": 17,
                  "Reggaeton": 18,
                  "Jazz": 19,
                  "Rock": 20,
                  "Ska": 21,
                  "Comedy": 22,
                  "Soul": 23,
                  "Soundtrack": 24,
                  "World": 25}


def connect_spotify(config):
    with open(config) as f:
        c = json.load(f)
        client_id = c["client_id"]
        client_secret = c["client_secret"]
        # username = c["sp_username"]

    # scope = 'playlist-modify-public'
    # redirect_uri = 'http://localhost:8888/callback/ '
    # token = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope=scope,
    #                      username=username)
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(auth_manager=client_credentials_manager)


def connect_db(config):
    """connecting to database"""
    with open(config) as f:
        config = json.load(f)

    conn = psycopg2.connect(
        host=config["db_host"],
        dbname=config["db_name"],
        port=config["db_port"],
        user=config["db_user"],
        password=config["db_pw"])
    return conn


def db_data(conn, table):
    """get data from db"""
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM  {table}')
    resp = cur.fetchall()
    return resp


def preprocess_data_single(raw_data, user_input):
    """@data: list of songs with their features from db
       @user_input: URI of songs for that recommendation should be done
       @return: pre-processed set of songs incl user_song"""

    # data = data[:10_000] # make data smaller not to crash RAM
    # data = data[:500_000]

    # get full song data from userinput URI
    user_song = [raw_song for raw_song in raw_data if raw_song[0] == user_input][0]

    # append user's song to the end for later indexing
    if user_song in raw_data:
        raw_data.remove(user_song)
    raw_data.append(user_song)

    # get rid of id (since it is string)
    indices = list(range(1, len(raw_data[0])))
    data_no_uri = [itemgetter(*indices)(entry) for entry in raw_data]

    encoded_genre_data = []
    for music in data_no_uri:
        music = list(music)
        if 'Children' in music[-1]:
            encoded_song = [mus for mus in music[:-1]]
            encoded_song.append(GENRE_ENCODING['Children Music'])
        else:
            encoded_song = [mus for mus in music[:-1]]
            encoded_song.append(GENRE_ENCODING[music[-1]])
        encoded_genre_data.append(encoded_song)

    # normalize everything except genre
    norm_data = normalize(encoded_genre_data).tolist()
    pre_data = []
    for n_song, c_song in zip(norm_data, encoded_genre_data):
        temp_data = copy.deepcopy(n_song[:-1])
        temp_data.append(c_song[-1])
        pre_data.append(temp_data)

    # apply weights (make sure the last value is 1! Because otherwise it will change genre)
    # danceability | energy | loudness | speechiness | acousticness | instrumentalness | liveness | valence | popularity | genre
    weights = [1, 1, 1, 1, 1, 1, 1, 1, 0, 1]
    weighted_norm_data = []
    for song_entry in pre_data:
        new_song = []
        for i, value in enumerate(song_entry):
            value = value * weights[i]
            new_song.append(value)
        weighted_norm_data.append(new_song)

    weighted_norm_data = [tuple(weighted_entry) for weighted_entry in weighted_norm_data]

    return weighted_norm_data


def preprocess_playlist(user_file, sp_conn):
    """
    :param sp_conn: spotify connection
    :param user_file: file path to csv file with uris :param sp_conn: authorized spotify connection
    :return:
    - list_of_songs: dictionaries of songs
    - song_matrix: np array with songs data ['danceability', 'energy', 'loudness',
                                            'speechiness', 'acousticness','instrumentalness',
                                            'liveness', 'valence']
    - uri_of_playlist: list of uris
    """

    list_of_songs = []  # store songname
    list_of_uris = []  # store uris for songs
    artist_name = []
    popularity = []
    limit = 100  # maxmum 100 at once
    playlist = pd.read_csv(user_file)[:10]
    l = len(playlist)  # amount of songs
    limitcount = int(l / limit)  # (#songs) /limit
    songleft = 0
    for i in range(l):
        # list_of_uris.append(result['tracks']['items'][0]['uri']) #get the uri of the track(song)
        list_of_uris.append(playlist['uri'][i])
    # To extract the uri by removing 'spotify:track:'
    uri_of_playlist = "".join(list_of_uris)
    uri_of_playlist = uri_of_playlist.split("spotify:track:")
    uri_of_playlist.pop(0)
    # Get all the features of the song
    for i in range(l):
        result = sp_conn.track(uri_of_playlist[i])
        list_of_songs.append([{"Song_name": result["name"]}])
        artist_name.append({"Artist": result["artists"][0]["name"]})  # get the artist_name
        popularity.append({"popularity": result['popularity']})  # get the popularity of the song
    for j in range(limitcount + 2):
        songleft = l - j * limit
        if songleft > limit:
            Features = sp_conn.audio_features(uri_of_playlist[j * limit:j * limit + limit])
            for i in range(limit):
                list_of_songs[j * limit + i][0].update(artist_name[i])
                list_of_songs[j * limit + i][0].update(popularity[i])
                list_of_songs[j * limit + i][0].update(Features[i])
                del list_of_songs[j * limit + i][0]['type']
                # del list_of_songs[j * limit + i][0]['id']
                del list_of_songs[j * limit + i][0]['uri']
                del list_of_songs[j * limit + i][0]['track_href']
                del list_of_songs[j * limit + i][0]['analysis_url']
        elif (songleft <= limit and songleft > 0):
            Features = sp_conn.audio_features(uri_of_playlist[j * limit:j * limit + songleft])
            for i in range(songleft):
                list_of_songs[j * limit + i][0].update(artist_name[i])
                list_of_songs[j * limit + i][0].update(popularity[i])
                list_of_songs[j * limit + i][0].update(Features[i])
                del list_of_songs[j * limit + i][0]['type']
                # del list_of_songs[j * limit + i][0]['id']
                del list_of_songs[j * limit + i][0]['uri']
                del list_of_songs[j * limit + i][0]['track_href']
                del list_of_songs[j * limit + i][0]['analysis_url']
        else:
            return list_of_songs, uri_of_playlist


def preprocess_data_playlist(raw_data, user_playlist_file, spotify_conn):
    """@raw_data: list of songs with their features from db
        @user_playlist: csv with uris
        @return: pre-processed set of songs incl user_song"""

    u_songs, u_uris = preprocess_playlist(user_playlist_file, spotify_conn)

    print(u_songs[3])
    print(u_uris[3])
    print(raw_data[0])

    # if user_song in raw_data:
    #     raw_data.remove(user_song)
    # raw_data.append(user_song)


def get_recomendation_KNN(cooked_data):
    """recommend top 5 nearest songs to users song
        prerequisite: users_song is the last one in list
                      data is normalized and weighted (cooked)"""
    nbrs = NearestNeighbors(n_neighbors=11, algorithm='auto').fit(cooked_data)
    distances, indices = nbrs.kneighbors(cooked_data)
    distances, indices = distances.tolist(), indices.tolist()
    # find n-cluster with the user song
    # user song was appended last, so its index is len(arr)-1
    user_song_i = len(cooked_data) - 1
    user_song_neigbors = []
    for ind_list in zip(distances, indices):
        if user_song_i in ind_list[1]:
            user_song_neigbors.append(list(ind_list))

    # select the best cluster -> smallest sum of distances
    sums = [sum(dist[0]) for dist in user_song_neigbors]
    # get index of smallest sum
    best_5 = user_song_neigbors[sums.index(min(sums))]
    best_5_d = best_5[0]
    best_5_i = best_5[1]
    best_5_songs = [cooked_data[i] for i in best_5_i]
    return best_5_songs, best_5_d


# def evaluate(rec_data):
#     """measure the average feature value of the playlist
#     of user and measure the average value of feautres
#     of recommended songs"""
#     avg_user = 0
#     recommender_avg = []
#     for out in rec_data:
#         if out[0] == INPUT_URI:
#             avg_user = round(mean(out[1:-1]), 4)  # exclude URI and genre
#         recommender_avg.append(round(mean(out[1:-1]), 4))
#
#     # calc percentages
#     percentages = []
#     for rec_avg in recommender_avg:
#         if rec_avg == avg_user:
#             percentages.append(1)
#         else:
#             percentages.append(1 - round((abs(rec_avg - avg_user) / avg_user), 4))
#
#     out = {rec[0]: prc for rec, prc in zip(rec_data, percentages)}
#
#     return out


if __name__ == '__main__':
    # INPUT_URI = input("Provide uri: ")

    data = db_data(connect_db('config.json'), 'spotify')
    sp_conn = connect_spotify('config.json')

    preprocess_data_playlist(data, 'Nishant2500.csv', sp_conn)

    # user_song_uri = INPUT_URI  # user's song is last
    #
    # # get data about user's song (name and artist)
    # res = sp_conn.track(user_song_uri)
    # artist = res['artists'][0]['name']
    # song = res['name']
    # print(f'User song: {song} by {artist}')
    # print(f'Searching thru {len(data)} songs only for you :)')
    #
    # # recommend
    # pre_proc_data = preprocess_data_single(data, INPUT_URI)
    # rec_5, distance = get_recomendation_KNN(pre_proc_data)
    # output = [list(data[pre_proc_data.index(rec)]) for rec in rec_5]
    # index_of_user_song = [i for i, song in enumerate(output) if song[0] == user_song_uri][0]
    # distance = [abs(distance[index_of_user_song] - dist) for dist in distance]
    # recomendations_uri = [[song[0], song[-1], song[-2], round(d, 4)] for d, song in zip(distance, output)]
    #
    # perc = evaluate(output)
    # print('Recomended Songs:')
    # print(f'Overall accuracy is: {round(mean(list(perc.values())[1:]), 2)}')
    # uri_artists = []
    # for recomendated_data in recomendations_uri:
    #     # ['1DoGY3bWXQEWqYc1jZ9Zbe', 'Movie', 0, 0.34]
    #
    #     res = sp_conn.track(recomendated_data[0])
    #     artist = res['artists'][0]['name']
    #     song = res['name']
    #     uri_artists.append([recomendated_data[0], artist, song, recomendated_data[1],
    #                         recomendated_data[2], recomendated_data[-1], round(perc[recomendated_data[0]], 2)])
    #
    # df = pd.DataFrame(uri_artists, columns=['URI', 'Artist', 'Song', 'Genre',
    #                                         'Popularity', 'Distance', 'Similarity in %'])
    # # perc is how similar the songs are acc. to the audio features
    # df.set_index('URI', inplace=True)
    # df.sort_values(by=['Similarity in %'], inplace=True, ascending=False)
    # print(tabulate(df, headers='keys', tablefmt='pretty'))
