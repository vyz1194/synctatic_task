from multiprocessing import Process,Queue,Pipe
from stage0 import sourcer
import pandas as pd
import yaml

with open('config.yml', 'r') as stream:
            config =yaml.load(stream)

def joiner(stage_1_conn):
    reciever,stage_0_conn = Pipe()
    p1 = Process(target=sourcer, args=(stage_0_conn,))
    p1.start()
    sources=reciever.recv()
    print("Stage 1")
    joined_sources=pd.merge(sources["1"],sources["2"],on=config['join_key'],how=config['join_type'])
    stage_1_conn.send(joined_sources)
    stage_1_conn.close()
    
