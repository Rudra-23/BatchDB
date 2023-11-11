import re
import pandas as pd
import os
import json

class UpdateValues:
    name = None
    updates = None
    query = None
    where_conditions = None

    def __init__(self, query) -> None:
        self.query = query

    def process_query(self):
        pattern = r'update\s+\{([^)]+)\}\s+in the table\s+([^"]+)\s+where\s+\{([^)]+)\};'
        match = re.search(pattern, self.query, re.IGNORECASE)

        if match:
            self.name = match.group(2)

            updates_str = match.group(1)
            self.updates = dict(re.findall(r'(\w+)\s*:\s*([^,]+)', updates_str))

            self.where_conditions = match.group(3) 

            return "VALID"
        else:
            return "INVALID QUERY"

    def is_valid_data(self, item):
        try:
            for k, v in self.updates.items():
                print(k, v, item["columns"][k])
                if item["columns"][k] == "str":
                    assert v[0] == '"' and v[-1] == '"'
                    self.updates[k] = str(v)[1:-1]
                elif item["columns"][k] == "int":
                    int(v)
                    self.updates[k] = int(v)
                elif item["columns"][k] == "float":
                    float(v)
                    self.updates[k] = float(v)
            return "Valid"
        except:
            return "Item datatype does not match"

    def is_valid(self):
        with open("./metadata.json", 'r') as json_file:
            metadata = json.load(json_file)

        for item in metadata:
            if item["table_name"] == self.name:
                return self.is_valid_data(item)        
                
        return "Table not found."
            

    def update_values(self):
        message =  self.is_valid()

        if message == "Table not found.":
            return message
        if message == "Item datatype does not match":
            return message

        file_name = f"./Data/{self.name}.csv"
        tmp_file = f"./TMP/Temp_{self.name}.csv"

        columns = False

        batch_size = 100  
        start_row = 0
        end_row = batch_size

        while True:
            df = pd.read_csv(file_name, skiprows=range(1, start_row), nrows=batch_size)

            if df.empty:
                break
            
            for index, item in df.iterrows():
                namespace = {}
                namespace.update(item)
                try:
                    if eval(self.where_conditions, namespace):
                        for key, value in self.updates.items():
                            item[key] = value
                        df.loc[index] = item
                except:
                    return "Incorrect syntax or variables."
            
            with open(tmp_file, 'a', newline="") as f:
                if not columns:
                    df.to_csv(f, header=True, index=False)
                else:
                    columns = True
                    df.to_csv(f, header=False, index=False)

            start_row = end_row
            end_row += batch_size

        os.replace(tmp_file, file_name)

        return "Items Updated."
