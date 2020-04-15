import time
from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server
from app import s3v
from lib import jsonRenderer
import json

profile = "default"


class dl_exporter(object):
    def __init__(self, get_table_dict):
        self.dictionary = json.loads(get_table_dict)
        pass

    def collect(self):
        for tab, v in self.dictionary['Tables'].items():
            key_list = []
            val_list = []
            for key, val in v[0].items():
                key_list.append(key.replace(" ", "_"))
                val_list.append(val)
            labels_names = key_list + ['Table']
            labels_values = val_list + [tab]
            g = GaugeMetricFamily("summary", 'dl-monitoring', labels=labels_names)
            g.add_metric(labels_values, 1)
            yield g


def __getstatus__():
    gc, sc = s3v.init_session(profile)
    db_list = s3v.get_db(gc)
    unhealthy_datasource = s3v.get_tab(sc, gc, db_list)
    return unhealthy_datasource


if __name__ == '__main__':
    start_http_server(8000)
    ds = jsonRenderer.json_response(__getstatus__())
    REGISTRY.register(dl_exporter(ds))
    while True:
        time.sleep(1)
