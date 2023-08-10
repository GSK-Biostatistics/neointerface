from neointerface import NeoInterface
import pandas as pd

def clean_dict(d: dict):
    res = {}
    for k, i in d.items():
        if pd.notna(i):
            if isinstance(i, str):
                res[k.lstrip()] = i.lstrip().lstrip('\"').rstrip('\"')
            else:
                res[k.lstrip()] = i
    return res

def csv_update_graph(interface: NeoInterface, df: pd.DataFrame):
    q = """
    UNWIND $data as row
    MATCH (x)
    WHERE id(x) = row['_id']
    //SET x = {}
    SET x = apoc.map.removeKeys(row,["_id"],{})
    """    
    params = {'data': [clean_dict(row) for row in df.to_dict(orient="records")]}
    # for row in params['data']:
    #     print(row)
    interface.query(q, params)