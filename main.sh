#!/usr/bin/env bash

while getopts 'n:e:' opt
do
  case $opt in
    n) NODES=$OPTARG ;;
    e) EDGES=$OPTARG ;;
  esac
done

python simple_kp/build_db.py data/data.db --nodes $NODES --edges $EDGES
uvicorn simple_kp.server:app --host 0.0.0.0 --port 5139 --reload
rm data/data.db
