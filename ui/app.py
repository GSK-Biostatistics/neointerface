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

w_tab_clear_db = tab_clear_db(neo)
w_tab_data_load = tab_data_load(neo)
w_tab_extract_entities = tab_extract_entities(neo)
w_tab_link_entities = tab_link_entities(neo)

tabs = pn.Tabs(
    ('Clear Database', w_tab_clear_db), 
    ('Data Load', w_tab_data_load), 
    ('Extract Nodes', w_tab_extract_entities),
    ('Link Nodes', w_tab_link_entities)
)

#on tab extract_entities activated
def tabs_on_extract_entities_activated(event):
    if event.obj.active == 2:
        with open("ui/cypher/get_labels.cypher", "r") as f:
            q = f.read()
        w_tab_extract_entities.extract_entities_from_class.options = [r["label"] for r in neo.query(q)]
watcher_extract_entities_activated = tabs.param.watch(tabs_on_extract_entities_activated, "active")

#on tab link_entities activated
def tabs_on_link_entities_activated(event):
    if event.obj.active == 3:
        with open("ui/cypher/get_labels.cypher", "r") as f:
            q = f.read()
        w_tab_link_entities.link_entities_left_class.options = [r["label"] for r in neo.query(q)]
        w_tab_link_entities.link_entities_right_class.options = w_tab_link_entities.link_entities_left_class.options
watcher_link_entities_activated = tabs.param.watch(tabs_on_link_entities_activated, "active")

pn.extension()
tabs.servable()