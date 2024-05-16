import pandas as pd
from tabulate import tabulate

class Project:
    def __init__(self, table_name, cols) -> None:
        self.table_name = table_name
        self.cols = cols

        self.tmp_dir = "./TMP/"
        self.data_dir = "./Data/"

    def process_table(self):
        try:
            file_name =  self.data_dir + f"{self.table_name}.csv"

            df = pd.read_csv(file_name)
            self.cols = self.cols if self.cols != [''] else list(df.columns)
            table = tabulate(df, headers= list(self.cols), tablefmt="fancy_grid")
            
            print(table)

            return "success"
        except:
            return "err"
        
    def display_values(self):
        status = self.process_table()

        if status == 'err':
            raise SyntaxError("Error: Some error occurred while projecting. Please check variables")


if __name__ == "__main__":
    
    while True:
        table = input("Enter table name [large] or [exit]: ")
        if table == 'exit':
            break
        
        obj = Project(table, [""])
        obj.display_values()
