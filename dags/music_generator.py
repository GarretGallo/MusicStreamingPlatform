from airflow.operators.empty import EmptyOperator
from airflow import DAG

from artist_generator import first_names

from datetime import datetime, timedelta
import pandas as pd
import random

from kafka_operator import KafkaProduceOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
}

num_rows = 50
output_file = './account_large_data.csv'

def generate_random_data(row_num):
    song = f"song{row_num:023cc4W}"
    album = f"album{row_num:023cc4W}"
    length = random.randint(1, 5)
    artist = f"artist{row_num:023cc4W}"
    genre = f"genre{row_num:023cc4W}"

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 3650))
    release_date = int(random_date.timestamp()*1000)

    return song, album, length, artist, genre, now, release_date

songs = []
albums = []
lengths = []
artists = []
genres = []
release_dates = []

def generate_music_data():
    row_num = 1
    while row_num <= num_rows:
        song, album, length, artist, genre, release_date = generate_random_data(row_num)
        songs.append(song)
        albums.append(album)
        lengths.append(length)
        artists.append(artist)
        genres.append(genre)
        release_dates.append(release_date)

        df  = pd.DataFrame({'song': songs,
                            'album': albums,
                            'length': lengths,
                            'artist': artists,
                            'genre': genres,
                            'release_date': release_dates})
        df.to_csv(output_file, index=False)
        print(f"CSV file '{output_file}' with {num_rows} rows created successfully!")

with DAG('music_generator',
         default_args=default_args,
         description='A DAG to generate music data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:
    start = EmptyOperator(task_id='start')

    generator_music_data = KafkaProduceOperator(
        task_id='generator_music_data',
        kafka_broker='kafka_broker:49092',
        kafka_topic='music_data')

    end = EmptyOperator(task_id='end')

    start >> generator_music_data >> end