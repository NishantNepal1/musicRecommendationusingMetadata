# Music Recommendation System using Metadata
TEAM: The United Nations for KAIST CS470 project
TEAM MEMBERS: Chloe McCracken, Nishant Nepal, Alexander Semenov, Junhao Zhou 
### Install dependencies:
```bash
pip install -r requirements.txt
```

### How fill_db_from_csv_works
#### Input 
- csv file with header like this: id,name,album,album_id,artists,artist_ids,track_number,disc_number,explicit,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,duration_ms,time_signature,year,release_date
  (Can be downloaded from https://www.kaggle.com/datasets/rodolfofigueroa/spotify-12m-songs)
- filled config file with database data
- ps: the duplicates are filtered before the csv entries are inserted into db
  - so we have to put one large csv into the db at once bcs obtaining ids and comparing with the csv ones takes 10 sek per 1000 entries (too long)
  - the issue that we cannot ignore duplicates while inserting into db is that the insertion method we are using "cur.copy_from" does not support it
#### Output
- lines from csv will be filled as rows in the relational database

### Performance
Some benchmarks:
CSV rows - time to fill the database
10.000 - 2.50 sec
12.300 - 3.02 sec
100.000 - 6.72 sec
500.000 - 32.16 sec

### Security notice
please do not push the config.json with keys to github. You can create a local "local_config.json" and store the keys there (add it to gitignore)
