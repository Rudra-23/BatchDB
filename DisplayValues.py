import re
import pandas as pd
from tabulate import tabulate

class DisplayValues:
    name = None
    cols = None
    query = None
    where_conditions = None

    def __init__(self, query) -> None:
        self.query = query

    def process_query(self):
        pattern = r'get\s+\(([^)]+)\)\s+in\s+([^"]+)\s+where\s+\(([^)]+)\)'
        match = re.search(pattern, self.query, re.IGNORECASE)

        if match:
            self.name = match.group(2)

            self.cols = match.group(1)
            self.cols = self.cols.split(", ")

            self.where_conditions = match.group(3) 

            return "VALID"
        else:
            return "INVALID QUERY"

    def display_values(self):
        file_name = f"./Data/{self.name}.csv"

        batch_size = 100  
        start_row = 0
        end_row = batch_size
        headers = True

        while True:
            df = pd.read_csv(file_name, skiprows=range(1, start_row), nrows=batch_size)

            cols = list(self.cols)

            data = []

            if df.empty:
                break
            
            for index, item in df.iterrows():
                namespace = {}
                namespace.update(item)
                try:
                    if eval(self.where_conditions, namespace):
                        selected_data = [item[col] for col in self.cols]
                        data.append(selected_data)
                except:
                    return "Incorrect syntax or variables."

            if data:
                if headers == True:
                    table = tabulate(data, headers=cols, tablefmt="fancy_grid")
                    headers = False
                else:
                    table = tabulate(data, tablefmt="fancy_grid")
                print(table)

            start_row = end_row
            end_row += batch_size

        return "Completed."
