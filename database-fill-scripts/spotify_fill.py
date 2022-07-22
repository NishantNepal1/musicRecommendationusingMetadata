"""IMPORTANT: this script is actively used in the final version"""

import io
import json
import time
import progressbar
import psycopg2
import csv


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


def parse_csv(filename):
    """
    Parse csv file
    :param filename:
    :return: headers dictionary {header_name:array_position}
            content: 2d array of lines of csv (split)
    """
    csv_content = []

    with open(filename, encoding='utf-8-sig') as f:
        headers = f.readline().strip().split(",")
        for line in csv.reader(f.readlines(), quotechar='"', delimiter=',',
                               quoting=csv.QUOTE_ALL, skipinitialspace=True):
            if len(line) == len(headers):
                csv_content.append(line)
            else:
                print('The following line in csv caused an error:'
                      '\nLenght is not equal to num. of headers')
                print(line)
                break
    return csv_content


def clean_csv_value(value):
    """ remove all non-visible \n in csv before inserting smth"""
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def fill_db(db_connection, table_name, file_data):
    start = time.time()
    cur = db_connection.cursor()

    HEADERS = ['id', 'danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness',
               'liveness', 'valence', 'popularity', 'genre']

    # reorder order to fit the db
    re_ordered = []
    for song in file_data:
        new_song = [
            song[3],
            song[6],
            song[8],
            song[12],
            song[14],
            song[5],
            song[9],
            song[11],
            song[17],
            song[4],
            song[0],
        ]
        re_ordered.append(new_song)

    # remove csv entries with same id
    current_ids = []
    filtered_data = []
    print('Removing duplicates ...')
    widgets = [progressbar.Percentage(), progressbar.Bar()]
    bar = progressbar.ProgressBar(widgets=widgets, max_value=len(re_ordered)).start()
    i = 0
    for row in re_ordered:
        if row[0] not in current_ids:
            filtered_data.append(row)
        current_ids.append(row[0])
        i += 1
        bar.update(i)
    bar.finish()
    print("Removing duplicates finished")

    # list of dictionaries with column names as keys and
    # value as row values to fit the csv_file_like_object
    q_args = []
    for line in filtered_data[0:len(filtered_data)]:
        q_dict_arg = {}
        for key, value in zip(HEADERS, line):
            q_dict_arg[key] = value

        q_args.append(q_dict_arg)

    # make a object csv and copy into db for best performance
    # see: copy_stringio() from https://hakibenita.com/fast-load-data-python-postgresql
    print("Inserting into db")
    csv_file_like_object = io.StringIO()

    for arg in q_args:
        csv_file_like_object.write('~'.join(map(clean_csv_value, arg.values())) + '\n')

    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, f'{table_name}', sep='~')

    # commit request
    db_connection.commit()
    end = time.time()
    print(f'Inserting finished.'
          f'\nTime required: {round(end - start, 2)} sec.')


if __name__ == '__main__':
    data = parse_csv('../data/SpotifyFeatures.csv')
    conn = connect_db('../config.json')
    fill_db(conn, 'spotify', data)
