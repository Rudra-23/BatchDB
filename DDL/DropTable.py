import re
import json
import os

class DropTable:
    name = None
    query = None

    def __init__(self, query) -> None:
        self.query = query

    def process_query(self):
        pattern = r"drop\s+table\s+(.+)"

        match = re.search(pattern, self.query, re.IGNORECASE)

        if match:
            self.name = match.group(1)
            return "VALID"
        else:
            return "INVALID QUERY"

    def drop_table(self):
        file_name = f"./Data/{self.name}.csv"
    
        try:
            os.remove(file_name)
        except FileNotFoundError:
            return "Table not found."

        with open('metadata.json', 'r') as metadata_file:
            existing_metadata = json.load(metadata_file)
        
        existing_metadata = [item for item in existing_metadata if item["table_name"] != self.name]

        with open('metadata.json', 'w') as metadata_file:
            json.dump(existing_metadata, metadata_file, indent=4)

        return "Table Dropped."
        
