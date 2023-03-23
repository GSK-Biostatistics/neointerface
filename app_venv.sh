venv/bin/python -m pip install --upgrade pip
venv/bin/python -m pip install -r requirements.txt
venv/bin/python -m pip install -r ui/requirements.txt
venv/bin/python -m panel serve ui/app.py --address="0.0.0.0" --port=8888 --autoreload --allow-websocket-origin=*