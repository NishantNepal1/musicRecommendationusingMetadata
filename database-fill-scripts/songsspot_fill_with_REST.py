"""IMPORTANT: this script is not used in the final version"""

import io
from typing import Optional, Any
import psycopg2
import json
import asyncio
import time
import aiohttp

TABLE_NAME = 'songsspot'


def connect_db(config_file):
    """ Connect to the PostgreSQL database server
        with given config """

    with open(config_file) as f:
        config = json.load(f)

    conn = None
    try:
        conn = psycopg2.connect(
            host=config["db_host"],
            dbname=config["db_name"],
            port=config["db_port"],
            user=config["db_user"],
            password=config["db_pw"])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn


def parse_json(path):
    # [{'id': uri, 'song': track['track_name'], 'artist': track['artist_name']}, ...]
    main_data = []
    with open(path) as f:
        data = json.load(f)
        for line in data['playlists']:
            for track in line['tracks']:
                uri = track['track_uri'].split(":")[-1]
                song_dict = {'id': uri, 'song': track['track_name'], 'artist': track['artist_name']}
                main_data.append(song_dict)
    return main_data


async def request_features(uri, session):  # uris
    FEATURES_BASE_URL = "https://api.spotify.com/v1/audio-features/"

    async with session.get(FEATURES_BASE_URL + uri) as response:
        data = await response.read()
        result = json.loads(data)
    # FEATURES_BASE_URL = 'https://api.spotify.com/v1/tracks/?ids='
    # async with session.get(FEATURES_BASE_URL + uris) as response:
    #     data = await response.read()
    #     result = json.loads(data)

    return result


async def get_features(config_path, songs_arr):
    with open(config_path) as f:
        config = json.load(f)
    headers = {
        'Authorization': f'Bearer {config["oauth"]}'
    }
    print(f'Size of songs_arr: {len(songs_arr)}')

    # split into blocks of 140 songs to avoid web api rate limits
    # https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    N = 140
    songs_chunks = [songs_arr[i * N:(i + 1) * N] for i in range((len(songs_arr) + N - 1) // N)]

    all_res = []
    for i, chunk in enumerate(songs_chunks[:3]):
        print(f'Chunk: {i+1}/{3}')
        time.sleep(20)
        my_conn = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(headers=headers, connector=my_conn) as session:
            tasks = []
            for song_dict in chunk:  # todo:
                uri = song_dict['id']
                task = asyncio.ensure_future(request_features(uri, session=session))
                tasks.append(task)
            res = await asyncio.gather(*tasks)
            all_res.append(res)
    print(all_res)
    return all_res


def extend_csv_with_features(songs_arr, features_list):
    extended_songs_arr = []
    for song_dict, features in zip(songs_arr, features_list):
        features_selected = {'danceability': features['danceability'], 'energy': features['energy'],
                             'key': features['key'], 'loudness': features['loudness'],
                             'mode': features['mode'], 'speechiness': features['speechiness'],
                             'acousticness': features['acousticness'], 'instrumentalness': features['instrumentalness'],
                             'liveness': features['liveness'], 'valence': features['valence'],
                             'tempo': features['tempo'], 'duration_ms': features['duration_ms']}
        extended_songs_arr.append(song_dict | features_selected)

    return extended_songs_arr


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def fill_database(db_connection, values):
    cur = db_connection.cursor()
    # make a object csv and copy into db for best performance
    # see: copy_stringio() from https://hakibenita.com/fast-load-data-python-postgresql
    csv_file_like_object = io.StringIO()

    # remove duplicates on csv
    values = [dict(t) for t in {tuple(d.items()) for d in values}]
    for arg in values:
        csv_file_like_object.write('|'.join(map(clean_csv_value, (
            arg['id'],
            arg['song'],
            arg['artist'],
            arg['danceability'],
            arg['energy'],
            arg['loudness'],
            arg['mode'],
            arg['speechiness'],
            arg['acousticness'],
            arg['instrumentalness'],
            arg['liveness'],
            arg['valence'],
            arg['tempo'],
            int(arg['key']),
            int(arg['duration_ms'])
        ))) + '\n')
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, TABLE_NAME, sep='|')
    # commit request
    db_connection.commit()
    cur.close()
    db_connection.close()


if __name__ == '__main__':
    start = time.time()
    json_path = '../data/mpd.slice.0-999.json'
    connection = connect_db("local_config.json")
    songs = parse_json(json_path)
    resp = asyncio.run(get_features("local_config.json", songs))
    songs = extend_csv_with_features(songs, resp)
    fill_database(connection, songs)
    end = time.time()
    print(f'The program took: {end - start}sec')
