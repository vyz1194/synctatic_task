from multiprocessing import Process,Pipe
import pandas as pd
import yaml
import io
import os

with open('config.yml', 'r') as stream:
            config =yaml.load(stream)
            #print(config)

def sourcer(stage_1_conn):
	source_1=pd.read_csv(config['source_file_1_path'])
	source_2=pd.read_csv(config['source_file_2_path'])
	sources={}
	sources["1"]=source_1
	sources["2"]=source_2
	stage_1_conn.send(sources)
	stage_1_conn.close()