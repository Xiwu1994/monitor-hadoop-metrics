import requests
import json
g_session = requests.session()


class OpenTSDBWrite(object):
    url = "http://localhost:4242/api/put?summary"

    @staticmethod
    def write_db(insert_data_list):
        # split insert_data_list 50
        for i in xrange(0, int(len(insert_data_list) / 50) + 1):
            insert_data = json.dumps(insert_data_list[i*50:(i+1)*50])
            print g_session.post(OpenTSDBWrite.url, data=insert_data).content
