from dagster import ConfigurableIOManager, InputContext, OutputContext
from pymongo import MongoClient

class MongoDBIOManager(ConfigurableIOManager):
    connection_string: str
    database_name: str

    def _get_collection(self, asset_key):
        client = MongoClient(self.connection_string)
        db = client[self.database_name]
        return db[asset_key.path[-1]]

    def handle_output(self, context: OutputContext, obj):
        collection = self._get_collection(context.asset_key)
        partition_key = context.asset_partition_key if context.has_partition_key else None
        if isinstance(obj, dict):
            obj['partition_key'] = partition_key
            collection.insert_one(obj)
        elif isinstance(obj, list):
            if all(isinstance(item, dict) for item in obj):
                for item in obj:
                    item['partition_key'] = partition_key
                collection.insert_many(obj)
            else:
                raise ValueError("All items in the list must be dictionaries")
        else:
            raise ValueError("Output must be a dictionary or a list of dictionaries")

    def load_input(self, context: InputContext):
        collection = self._get_collection(context.asset_key)
        partition_key = context.asset_partition_key if context.has_partition_key else None
        query = {'partition_key': partition_key} if partition_key else {}
        data = list(collection.find(query))
        if len(data) == 1:
            return data[0]
        return data
