import re
import csv
import json

class InsertValues():
    name = None
    values = None
    query = None

    def __init__(self, query) -> None:
        self.query = query

    def process_query(self):
        pattern = r"insert\s+into\s+(.+)\s+\((.+)\)"

        match = re.search(pattern, self.query, re.IGNORECASE)

        if match:
            self.name = match.group(1)
            
            values = match.group(2)
            values = values.split(',')

            self.values = []
            for val in values:
                val = val.strip()
                self.values.append(val)

            return "VALID"
        else:
            return "INVALID QUERY"
        
    def is_valid(self, name):
        with open("./metadata.json", 'r') as json_file:
            metadata = json.load(json_file)

        for item in metadata:
            if item["table_name"] == name:
                return item
        return None  
    
    def insert_values(self):
        
        item = self.is_valid(self.name)

        if item == None:
            return "No Table Found"
        
        for i, v  in zip(item['columns'].values(), self.values):
            try:
                if i == "str":
                    str(v)
                if i == "int":
                    int(v)
                if i == "float":
                    float(v)
                    
            except:
                return "item's datatype does not match"
            

        file_name = "./Data/" + self.name + '.csv'
        with open(file_name, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.values)

        return "Item Added."
        
        


        
        
