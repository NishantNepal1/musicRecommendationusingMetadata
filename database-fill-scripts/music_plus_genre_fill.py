"""IMPORTANT: this script is not used in the final version"""

import io
import json

import pandas as pd
import progressbar
import psycopg2
import os


def connect_db(config_file):
    """ Connect to the PostgreSQL database server
        with given config """

    with open(config_file) as f:
        config = json.load(f)

    connection = None
    try:
        connection = psycopg2.connect(
            host=config["db_host"],
            dbname=config["db_name"],
            port=config["db_port"],
            user=config["db_user"],
            password=config["db_pw"])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return connection


def parse_xlsx(file_path):
    """parse a single xlsx to list of uris and genre"""
    dfs = pd.read_excel(file_path)
    genre = file_path.split('Songs.xlsx')[0].split('/')[-1].lower()
    uris = dfs.iloc[:, 0].tolist()
    uris = [value.split('spotify:track:')[-1] for value in uris]
    return uris, genre


def parse_all_xls_of_dir(dir_path):
    """apply parse_xlsx to all files in directory"""
    results = {}  # genre:list of uris
    for filename in os.listdir(dir_path):
        f = os.path.join(dir_path, filename)
        if os.path.isfile(f):
            uris, genre = parse_xlsx(f'{dir_path}/{filename}')
            results[genre] = uris
    return results


def clean_csv_value(value):
    """ remove all non-visible \n in csv before inserting smth"""
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def fill_db(db_connection, table_name, data):
    cur = db_connection.cursor()
    full_data = []
    not_found_uri = []
    headers = ['id', 'danceability', 'energy', 'loudness', 'speechiness', 'acousticness',
               'instrumentalness', 'liveness', 'valence', 'tempo', 'mode', 'key',
               'duration_ms', 'popularity', 'genre']

    counter = 0
    for key, value in data.items():
        counter += 1
        print(f'Genre: {key}')
        print(f'{counter}/11')
        bar = progressbar.ProgressBar(max_value=len(value)).start()
        counter = 0
        for uri in value:
            query = f'SELECT * from music WHERE id = \'{uri}\';'
            cur.execute(query)
            resp = cur.fetchall()
            if len(resp) == 0:
                not_found_uri.append(uri)
            else:
                data = list(resp[0])
                data.append(key)  # genre
                full_data.append(data)
            counter += 1
            bar.update(counter)

    # write down not found uris
    with open('notfounduri.txt', 'w') as f:
        for nonuri in not_found_uri:
            f.write(f'{nonuri}\n')

    # list of dictionaries with column names as keys and
    # value as row values
    q_args = []

    FROM = 0
    TILL = len(full_data)

    for line in full_data[FROM:TILL]:
        q_dict_arg = {}
        for key, value in zip(headers, line):
            q_dict_arg[key] = value

        q_args.append(q_dict_arg)

    # make a object csv and copy into db for best performance
    # see: copy_stringio() from https://hakibenita.com/fast-load-data-python-postgresql
    csv_file_like_object = io.StringIO()

    for arg in q_args:
        csv_file_like_object.write('~'.join(map(clean_csv_value, arg.values())) + '\n')

    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, f'{table_name}', sep='~')
    # commit request
    db_connection.commit()


if __name__ == '__main__':
    # genre:list of uris
    res = parse_all_xls_of_dir(dir_path='../data/genres')
    conn = connect_db('config.json')
    fill_db(conn, 'music_plus_genre', res)
