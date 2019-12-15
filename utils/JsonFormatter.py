import re
class JsonFormatter():
    def format(self, json):
        json = str(json)
        if("}{" in json):
            print("1 \n")
            json = json.replace("}{", "},{")
            json = "[" + json + "]"
            print(json)
        return json