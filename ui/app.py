from ui.src.tab_clear_db import tab_clear_db
from ui.src.tab_data_load import tab_data_load
from ui.src.tab_extract_entities import tab_extract_entities
from ui.src.tab_cont_link_entities import tab_link_entities
from neointerface.neointerface import NeoInterface
import panel as pn
import pandas as pd
import io
import json

neo = NeoInterface()

tabs = pn.Tabs(
    ('Clear Database', tab_clear_db(neo)), 
    ('Data Load', tab_data_load(neo)), 
    ('Extract Nodes', tab_extract_entities(neo)),
    ('Link Nodes', tab_link_entities(neo))
)

#on tab extract_entities activated
def tabs_on_extract_entities_activated(event):
    if event.obj.active == 2:
        extract_entities_from_class.options = [r["label"] for r in neo.query(
            """
            call db.labels() yield label return label order by label            
            """
        )]
watcher_extract_entities_activated = tabs.param.watch(tabs_on_extract_entities_activated, "active")

#on tab link_entities activated
def tabs_on_link_entities_activated(event):
    if event.obj.active == 3:
        link_entities_left_class.options = [r["label"] for r in neo.query(
            """
            call db.labels() yield label return label order by label            
            """
        )]
        link_entities_right_class.options = link_entities_left_class.options
watcher_link_entities_activated = tabs.param.watch(tabs_on_link_entities_activated, "active")

pn.extension()
tabs.servable()