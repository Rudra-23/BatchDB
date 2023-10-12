import re
import pandas as pd
import os

class DeleteValues:
    name = None
    query = None
    where_conditions = None

    def __init__(self, query) -> None:
        self.query = query

    def process_query(self):
        pattern = r'delete\s+from\s+([^"]+)\s+where\s+\(([^)]+)\)'
        match = re.search(pattern, self.query, re.IGNORECASE)

        if match:
            self.name = match.group(1)
            self.where_conditions = match.group(2)

            return "VALID"
        else:
            return "INVALID QUERY"

    def delete_values(self):
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

            rows_to_delete = []

            for index, item in df.iterrows():
                namespace = {}
                namespace.update(item)
                try:
                    if eval(self.where_conditions, namespace):
                        rows_to_delete.append(index)
                except:
                    return "Incorrect syntax or variables."
                
            df = df.drop(rows_to_delete)
            
            with open(tmp_file, 'a', newline="") as f:
                if not columns:
                    df.to_csv(f, header=True, index=False)
                else:
                    columns = True
                    df.to_csv(f, header=False, index=False)

            start_row = end_row
            end_row += batch_size

        os.replace(tmp_file, file_name)

        return "Items Deleted."
