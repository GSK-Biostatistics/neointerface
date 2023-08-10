import io
import panel as pn
import pandas as pd
from src.utils import csv_update_graph
from neointerface import NeoInterface
import csv 
neo = NeoInterface()

pn.extension()
file_input = pn.widgets.FileInput(accept='.csv')
container = pn.Row(file_input)

def do(event):     
    if event.new is not None:
        df = pd.read_csv(io.BytesIO(event.new), sep=",")
        # print(df)
        csv_update_graph(neo, df)        
        file_input = pn.widgets.FileInput(accept='.csv')
        container[0] = file_input        
        watcher = file_input.param.watch(do, "value")

watcher = file_input.param.watch(do, "value")

container.servable()