from multiprocessing import Process,Queue,Pipe
from stage2 import dropper
import pandas as pd
import string
import yaml

with open('config.yml', 'r') as stream:
            config =yaml.load(stream)
            #print(config)

def filter():
    reciever,stage_2_conn = Pipe()
    p3 = Process(target=dropper, args=(stage_2_conn,))
    p3.start()
    #print("stage 3 started")
    dropped=reciever.recv()
    print("stage 3")
    #print(dropped)
    #print("succefully dropped")
    filtered=dropped

    print("Before Filtering")
    print(filtered)
    columns_to_be_filtered=config['filter_columns']
    for column_to_be_filtered,filter_condition in columns_to_be_filtered.items():
        if(filter_condition['filter_type'].keys()[0]=='range'):
    	   upper_bound=filter_condition['filter_type']['range']['upper_bound']
    	   lower_bound=filter_condition['filter_type']['range']['lower_bound']
    	   filtered=filtered[(lower_bound<filtered[column_to_be_filtered]) & (filtered[column_to_be_filtered]<upper_bound)]
        elif(filter_condition['filter_type'].keys()[0]=='values'):
            filtered=filtered[filtered[column_to_be_filtered].isin(filter_condition['filter_type']['values'])]
    print("After Filtering")
    print(filtered)
    


if __name__ == '__main__':
    filter()
