#!/usr/bin/env python
# coding: utf-8

# HEHAHAHAHAHAHAAA


import sys
get_ipython().system('pip install elasticsearch')
from elasticsearch import Elasticsearch
from datetime import datetime

ES_HOST = "https://search-osler-l4nn67sl7gsi74fipp6aagc5tu.us-east-1.es.amazonaws.com"

RECORDS_PER_PAGE = 100
# experiment_id = "1583588360215"
query = "*"
b = '''{
    "_source": ["test_id", "method_names", "data", "tpc"],
    "sort": [{ "test_id" : {"order" : "desc"}}]
}'''

es = Elasticsearch(hosts=ES_HOST)
res = es.search(index="osler-index", q=query, body=b, size=RECORDS_PER_PAGE)
# res = es.search(index="osler-index", q=experiment_id, body=b, size=RECORDS_PER_PAGE)

out = res['hits']['hits']
hits = len(out)

serial_no = 1
arr_index = 0
dpv_count = 0
ocp_count = 0
hct_count = 0
ca_count = 0

print("=================================================================================================")
print("{0:6s}\t{1:16s}\t{2:16s}\t{3:27s}\t{4:40s}".format("Sr. No", "TPC ID", "Experiment ID", "DateTime", "Methods"))
print("=================================================================================================")

while hits != 0:
    hits -= 1
    method_names = out[arr_index]['_source'].get('method_names')
    if not method_names:
        method_names = set()
        raw_data = out[arr_index]['_source'].get('data')
        if raw_data and raw_data[0].get('method'):
            for d in raw_data:
                method_names.add(d.get('method'))
    if out[arr_index]['_source']['test_id'] not in ['T_3','T_2']:
        print("{0:6s}\t{1:16s}\t{2:16s}\t{3:27s}\t{4:40s}".format(
          str(serial_no),
    #               "               ",
          out[arr_index]['_source']['tpc'],
    #               "            ",
          out[arr_index]['_source']['test_id'],
    #               "            ",
          str(datetime.fromtimestamp(int(out[arr_index]['_source']['test_id'])/1000)),
    #               "            ",
              ", ".join(method_names)))
    if ('DPV' in method_names):
        dpv_count = dpv_count + 1
    if ('OCP' in method_names):
        ocp_count = ocp_count + 1
    if ('HCT' in method_names):
        hct_count = hct_count + 1
    if('CA' in method_names):
        ca_count = ca_count + 1            

    serial_no = serial_no + 1
    arr_index = arr_index + 1

#     for exp in out:
#         # Check if field `method_names` is present
#         method_names = exp['_source'].get('method_names')
#         if not method_names:
#             method_names = set()
#             # Check if field `method` is present data array
# #             print(">", exp['_source']['test_id'])
#             raw_data = exp['_source'].get('data')

#             if raw_data and raw_data[0].get('method'):
#                 for d in raw_data:
#                     method_names.add(d.get('method'))

#         print("{0:10s}\t{1:16s}\t{2:30s}\t{3:20s}".format(
#               str(serial_no),
# #               "               ",
#               exp['_source']['test_id'],
# #               "            ",
#               str(datetime.fromtimestamp(int(exp['_source']['test_id'])/1000)),
# #               "            ",
#               ", ".join(method_names)))

#         if ('DPV' in method_names):
#             dpv_count = dpv_count + 1
#         if ('OCP' in method_names):
#             ocp_count = ocp_count + 1
#         if ('HCT' in method_names):
#             hct_count = hct_count + 1
#         if('CA' in method_names):
#             ca_count = ca_count + 1            

#         serial_no = serial_no + 1
#     res = es.scroll(scroll_id=res['_scroll_id'], scroll="1m")
#     hits = len(res['hits']['hits'])
#     if hits > 0:
#         out = res['hits']['hits']

# es.clear_scroll(scroll_id=res['_scroll_id'])
# serial_no = serial_no - 1

print("\n\n")
print("{0:50s}\t{1:4d}".format("Number of DPV experiments: ", dpv_count))
print("{0:50s}\t{1:4d}".format("Number of OCP experiments: ", ocp_count))
print("{0:50s}\t{1:4d}".format("Number of HCT experiments: ", hct_count))
print("{0:50s}\t{1:4d}".format("Number of CA experiments: ", ca_count))
print("\n\n")
print("{0:50s}\t{1:4d}".format("Sum of (DPV + OCP + HCT + CA) experiments: ", dpv_count+ocp_count+hct_count+ca_count))
print("{0:50s}\t{1:4d}".format("Total number of experiments: ", serial_no - 1))

# " Case 1: when no. of experiments == sum of (DPV, HCT, OCP, CA)
#   Reason: each experiment done by the TPC contains only one type of method

#   Case 2: when no. of experiments > sum of (DPV, HCT, OCP, CA)
#   Reason: one or more experiments does not contain the field data.method (eg. test_id 1574439698029)

#   Case 3: when no. of experiments < sum of (DPV, HCT, OCP, CA)
#   Reason: one or more experiment done by the TPC contain multiple methods
# "

