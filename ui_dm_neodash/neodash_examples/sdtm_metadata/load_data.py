import os
import pandas as pd
from neointerface import NeoInterface

neo = NeoInterface()
neo.clean_slate()
df = pd.read_csv("ui_dm_neodash/neodash_examples/sdtm_metadata/SDTMIG_v3.2.csv")
neo.load_df(df, 'Variable')

neo.query(f"MERGE(x:`Upload Updates Host`{{value: '{os.getenv('UPLOAD_UPDATES_HOST')}'}})")