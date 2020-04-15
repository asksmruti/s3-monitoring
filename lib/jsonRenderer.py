import json
from datetime import datetime


def json_response(dict_list_val):
    s_dict = {}
    o_dict = {}
    s_dict['Version'] = 1.0
    s_dict['Timestamp'] = str(datetime.now())

    for index, object in enumerate(dict_list_val['Table']):
        i_dict = {}
        i_dict['Database'] = dict_list_val['Database'][index]
        i_dict['S3 Location'] = dict_list_val['S3 Location'][index]
        i_dict['Last updated'] = dict_list_val['Last updated'][index]
        o_dict.setdefault(object, []).append(i_dict)
    s_dict['Tables'] = o_dict
    return json.dumps(s_dict, indent=4)
