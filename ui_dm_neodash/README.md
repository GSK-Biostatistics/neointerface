# User Interfaces based on [NeoDash](https://github.com/neo4j-labs/neodash)
* deploy the ui_dd_csv application for loading updated data into the graph e.g. by runnning 
python -m panel serve ui_dm_neodash/ui_dd_csv/app.py --address="0.0.0.0" --port=8888 --autoreload --allow-websocket-origin=* &
(requires env variables NEO4J_HOST, NEO4J_USER and NEO4J_PASSWORD)
* run the load_data.py of the corresponding example in neodash_examples folder (requires UPLOAD_UPDATES_HOST env variable with the url to the upload updates app e.g. http://0.0.0.0:8888/app, as well as env variables NEO4J_HOST, NEO4J_USER and NEO4J_PASSWORD)
* deploy NeoDash dashboard from a json file in the corresponding folder of neodash_examples