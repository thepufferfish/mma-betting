import json
from dagster import FilesystemIOManager

class JSONIOManager(FilesystemIOManager):
    def handle_output(self, context, obj):
        path = self._get_path(context)
        path = path + '.json'
        with open(path, 'w') as f:
            json.dump(obj, f, indent=4)

    def load_input(self, context):
        path = self._get_path(context)
        path = path + '.json'
        with open(path, 'r') as f:
            return json.load(f)