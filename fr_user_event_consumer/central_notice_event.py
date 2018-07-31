import json

class CentralNoticeEvent:

    def __init__( self, json_string ):
        self._raw_json = json_string
        self._data = json.loads( json_string )