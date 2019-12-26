from multiprocessing import Process,Queue,Pipe
from stage1 import joiner
import yaml
import pandas as pd

with open('config.yml', 'r') as stream:
            config =yaml.load(stream)
            #print(config)

def dropper(stage_2_conn):
    reciever,stage_1_conn = Pipe()
    p2 = Process(target=joiner, args=(stage_1_conn,))
    p2.start()
    joined_sources=reciever.recv()
    print("Stage 2")
    after_dropping=joined_sources.drop(config['coloumns_to_drop'],axis=1)
    stage_2_conn.send(after_dropping)
    stage_2_conn.close()

