"""IMPORTANT: this script is actively used in the final version"""

import csv
import io
import json
import time

import progressbar
import psycopg2
import spotipy


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


def clean_csv_value(value):
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def csv_to_dict(csv_file):
    # needed to create csv like object for copy_from()
    res = []
    with open(csv_file) as f:
        header = f.readline().strip().split(',')
        for line in f.readlines():
            dic = {}
            for i, value in enumerate(line.split(",")):
                dic[header[i]] = value.strip()
            res.append(dic)
    return res


def connect_spotify(config):
    with open(config) as c:
        config = json.load(c)
    client_credentials_manager = spotipy.SpotifyClientCredentials(
        client_id=config['client_id'],
        client_secret=config['client_secret'])
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)


def insert_csv_into_db(config_file, table_name, data_file):
    start = time.time()
    db_connection = connect_db(config_file)
    cur = db_connection.cursor()

    # remove data that is already in the database (acc. to ids that are PK)
    query = f'SELECT id FROM {table_name};'
    cur.execute(query)
    db_ids = cur.fetchall()
    db_ids_clean = []
    for ids in db_ids:
        db_ids_clean.append(ids[0])

    # remove duplicates in file (in the current segment of data we use)
    print('Removing duplicates...')
    widgets = [progressbar.Percentage(), progressbar.Bar()]
    bar = progressbar.ProgressBar(widgets=widgets, max_value=170639).start()
    i = 1
    with open(data_file, 'r') as f, open('../data/clean_songs.csv', 'w') as out_file:
        reader = csv.reader(f, delimiter=',')
        header = next(reader, None)
        writer = csv.writer(out_file, delimiter=',')
        writer.writerow(header)
        current_ids = []
        dirty_rows = []
        for row in reader:
            # if i == 5:
            #     break
            # if i < :
            #     i += 1
            #     continue
            if len(row) != 15 or "" in row:
                dirty_rows.append(row[6])
                continue
            # if i == 1000:
            #     break
            if row[6] not in current_ids and row[6] not in db_ids_clean \
                    and row[-3] != '0':
                writer.writerow(row)
            current_ids.append(row[6])
            bar.update(i)
            i += 1
    bar.finish()
    print('Removing duplicates finished')
    print("Import data into db")

    csv_as_dict = csv_to_dict('../data/clean_songs.csv')
    csv_file_like_object = io.StringIO()

    for arg in csv_as_dict:
        csv_file_like_object.write('~'.join(map(clean_csv_value, (
            arg['id'],
            float(arg['danceability']),
            float(arg['loudness']),
            float(arg['speechiness']),
            float(arg['acousticness']),
            float(arg['instrumentalness']),
            float(arg['liveness']),
            float(arg['valence']),
            float(arg['tempo']),
            float(arg['mode']),
            int(arg['key']),
            int(arg['duration_ms']),
            int(arg['popularity'])
        ))) + '\n')
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, table_name, sep='~')
    # commit request
    db_connection.commit()
    end = time.time()
    print("Import data finished")
    print(f'Time for insert_csv_into_db: {end - start} sec.')
    # print(f'Dirty:\n{dirty_rows}')


if __name__ == '__main__':
    insert_csv_into_db('../config.json', 'music', '../data/music_features-data.csv')
