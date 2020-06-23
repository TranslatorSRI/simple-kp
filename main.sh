#!/usr/bin/env bash

data/build_db.py --origin FOTR data/FOTR.db
data/build_db.py --origin TT data/TT.db
data/build_db.py --origin ROTK data/ROTK.db
uvicorn simple_kp.server:app --host 0.0.0.0 --port 5139
