from functions.etl_pbp import extract_files, create_spark_session, get_location_of_ball
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType,FloatType,DateType
from pyspark.sql.functions import *

spark = create_spark_session()

urls_dwd = ['https://github.com/sealneaward/nba-movement-data/raw/master/data/01.01.2016.CHA.at.TOR.7z']
folder_raw = '/workspace/azure_pocket/raw_data' 
folder_tmp_path = "/workspace/azure_pocket/raw_data/tmp" # store json temp

game_files = extract_files(folder_raw=folder_raw,folder_tmp_path=folder_tmp_path,urls=urls_dwd)

movement = get_location_of_ball(folder_tmp_path=folder_tmp_path,game_files=game_files[0])

    
# create our schema to upload data
schema_location_of_ball_and_teams = StructType([ \
    StructField("team_id",LongType(),True),
    StructField("player_id",LongType(),True),
    StructField("location_x",FloatType(),True),
    StructField("location_y",FloatType(),True),
    StructField("location_z", FloatType(),True),
    StructField("game_clock", FloatType(),True),
    StructField("shot_clock", FloatType(),True),
    StructField("period", IntegerType(),True),
    StructField("game_id", StringType(),True),
    StructField("event_id", StringType(),True),
    StructField("game_date", StringType(),True)
    ]
)

teste = movement * 15

teste_df = spark.createDataFrame(teste,schema=schema_location_of_ball_and_teams)

teste_df.write.parquet('processed_files/')