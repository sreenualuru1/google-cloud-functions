import json
import pandas as pd


def read_csv_file(request):
   req_obj = request.get_json(silent=True)
   print(req_obj)
