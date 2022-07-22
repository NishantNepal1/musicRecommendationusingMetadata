"""IMPORTANT: this script is not used in the final version"""

import json
import random
import time
from typing import Optional, Any
import io

import psycopg2
import progressbar
import psycopg2.extras

TABLE_NAME = 'songs'
ignored_csv_entries = []


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


def parse_csv(filename):
    """
    Parse csv file
    :param filename:
    :return: headers dictionary {header_name:array_position}
            content: 2d array of lines of csv (split)
    """
    headers = {}
    csv_content = []

    with open(filename) as f:
        raw_header_arr = f.readline().strip().split(",")
        for i in range(len(raw_header_arr)):
            headers[raw_header_arr[i]] = i

        all_lines = f.readlines()
        print(f'Lines to analyse and fill the database with: {len(all_lines)}')
        for line in all_lines:
            # bcs of [artist1,artist2] split fails because we do not want
            # to consider "," inside object arrays like artists
            # so we set thoose "," inside brackets to # to not split on them
            line = line.replace("[", '{').replace("]", "}")

            line = line.strip().split(",")
            if len(line) != len(headers):
                ignored_csv_entries.append(line)
            else:
                csv_content.append(line)

    return headers, csv_content


def filter_headers_for_db(db_connection, headers):
    """
    take all columns from csv for that a columns in the database exist
    :param db_connection:
    :param headers:
    :return: list of column names and their orignal index in csv that can be used for
            filling the database and order of the columns in the database
    """

    cur = db_connection.cursor()
    # get tables columns names
    cur.execute(""" SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = 'songs';""")
    table_header = [value[0] for value in cur.fetchall()]

    # make intercept with csv headers
    filtered_headers = [value for value in headers.keys() if value in table_header]
    filtered_headers_dict = {}
    for value in filtered_headers:
        filtered_headers_dict[value] = headers[value]

    return filtered_headers_dict


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def fill_database(db_connection, headers, csv_content):
    cur = db_connection.cursor()
    # list of dictionaries with column names as keys and
    # value as row values
    q_args = []

    FROM = 0
    TILL = len(csv_content)

    start = time.time()
    with progressbar.ProgressBar(max_value=TILL - FROM) as bar:
        for i, line in enumerate(csv_content[FROM:TILL]):
            filtered_csv_values = []
            for col_name in headers.keys():
                if "id" in col_name:
                    # sometimes french songs have "'" in their name
                    # e.g Elle Prend L'boulevard Magenta
                    filtered_csv_values.append(line[headers[col_name]])
                else:
                    # convert not id, name and artists values to float
                    filtered_csv_values.append(float(line[headers[col_name]]))

            # convert to str to remove '' and convert back to list
            headers_keys = str(list(headers.keys())).replace("'", "").strip('][').split(', ')

            # dictionary as row e.g {'id': '7ubwgEls7ltItwIRp2ObOP', 'danceability': 0.731,...}
            q_dict_arg = {}
            for key, value in zip(headers_keys, filtered_csv_values):
                q_dict_arg[key] = value

            q_args.append(q_dict_arg)
            bar.update(i)

        # make a object csv and copy into db for best performance
        # see: copy_stringio() from https://hakibenita.com/fast-load-data-python-postgresql
        csv_file_like_object = io.StringIO()
        print(f'\nExample output to check its presence in db: {q_args[-3]}')

        # remove duplicates on csv
        q_args = [dict(t) for t in {tuple(d.items()) for d in q_args}]
        for arg in q_args:
            csv_file_like_object.write('|'.join(map(clean_csv_value, (
                arg['id'],
                arg['danceability'],
                arg['energy'],
                arg['loudness'],
                arg['speechiness'],
                arg['acousticness'],
                arg['instrumentalness'],
                arg['liveness'],
                arg['valence'],
                arg['tempo'],
                arg['time_signature'],
                arg['mode'],
                int(arg['key']),
                int(arg['duration_ms']),
            ))) + '\n')
        csv_file_like_object.seek(0)
        cur.copy_from(csv_file_like_object, f'{TABLE_NAME}', sep='|')
        # commit request
        db_connection.commit()

    cur.close()
    db_connection.close()
    end = time.time()

    print(f'Insertion of data finished.\nTime required: {end - start}')


if __name__ == '__main__':
    connection = connect_db("../config.json")
    header, data = parse_csv("../data/tracks_features.csv")
    filtered_head = filter_headers_for_db(connection, header)
    fill_database(db_connection=connection, headers=filtered_head, csv_content=data)
    print(f'Ignored csv lines because of formatting: {len(ignored_csv_entries)}')
    ran_int = random.randint(0, len(ignored_csv_entries))
    print(ignored_csv_entries[ran_int])
