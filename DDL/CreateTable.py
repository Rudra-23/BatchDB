import re
import csv
import json
import os

class CreateTable:
    name = None
    constraints = None
    cols = None
    query = None

    def __init__(self, query) -> None:
        self.query = query

    def process_query(self):
        pattern = r"create\s+table\s+(.+)\s+\((.+)\)"

        match = re.search(pattern, self.query, re.IGNORECASE)

        if match:
            self.name = match.group(1)
            
            columns = match.group(2)
            columns = columns.split(',')

            self.cols = {}
            for col in columns:
                col = col.strip()
                name, data_type = col.split(" ")
                self.cols[name] = data_type
            
            return "VALID"
        else:
            return "INVALID QUERY"

    def create_table(self):
        file_name = f"./Data/{self.name}.csv"
    
        if os.path.isfile(file_name):
            return "Table Already Exists."
        
        column_names = list(self.cols.keys())
        data_types = list(self.cols.values())

        with open(file_name, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([f"{name}" for name in column_names])


        metadata = {}
        metadata['table_name'] = self.name
        metadata['columns'] = self.cols

        with open('metadata.json', 'r') as metadata_file:
            existing_metadata = json.load(metadata_file)
        
        existing_metadata.append(metadata)

        with open('metadata.json', 'w') as metadata_file:
            json.dump(existing_metadata, metadata_file, indent=4)

        return "Table Created."
        
