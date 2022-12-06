python -m pip install -r requirements.txt --user
python -m pip install -r ui/requirements.txt --user
python -m panel serve ui/app.py --address="0.0.0.0" --port=8888 --autoreload --allow-websocket-origin=*