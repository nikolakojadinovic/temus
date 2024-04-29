from flask import Flask, jsonify, request
import time
from sqlalchemy import text
import json
from collections import OrderedDict

app = Flask(__name__)

if __name__ == "__main__":
    app.run(port=3001, debug = True)