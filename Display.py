import pandas as pd
from tabulate import tabulate

class DisplayValues:
    def __init__(self, table_name, cols) -> None:
        self.table_name = table_name
        self.cols = cols

    def display_values(self):
        file_name = f"./Data/{self.table_name}.csv"

        batch_size = 100  
        start_row = 0
        end_row = batch_size
        headers = True

        while True:
            df = pd.read_csv(file_name, skiprows=range(1, start_row), nrows=batch_size)

            cols = []
            data = []

            if df.empty:
                break
            
            for _, item in df.iterrows():
                if len(self.cols) == 0:
                    data.append(item.values)
                    cols = item.keys()
                else:
                    selected_data = [item[col] for col in self.cols]
                    cols = self.cols
                    data.append(selected_data)

            if data:
                if headers == True:
                    table = tabulate(data, headers=cols, tablefmt="fancy_grid")
                    headers = False
                else:
                    table = tabulate(data, tablefmt="fancy_grid")
                print(table)

            start_row = end_row
            end_row += batch_size

if __name__ == "__main__":
    obj = DisplayValues("_filtered__sorted__joined_student_athlete", ["student.age", "student.name", "athlete.weight"])
    obj.display_values()
