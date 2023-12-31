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

            batch_size = 100
            start_row = 0
            end_row = batch_size

            df = pd.read_csv(file_name, nrows = batch_size, skiprows= range(1, start_row))
            
            header = True

            self.cols = self.cols if self.cols != [''] else list(df.columns)

            if df.empty:
                table = tabulate([self.cols], tablefmt="fancy_grid", headers = "firstrow")
                print(table)
                return "success"

            while df is not None and not df.empty:
                data = []
                
                for _, item in df.iterrows():
                    if len(self.cols) == 0:
                        data.append(item.values)
                    else:
                        selected_data = [item[col] for col in self.cols]
                        data.append(selected_data)

                if data:
                    if header == True:
                        header = False
                        table = tabulate(data, headers= list(self.cols), tablefmt="fancy_grid")
                    else:
                        table = tabulate(data, tablefmt="fancy_grid")
                    print(table)

                start_row = end_row + 1
                end_row += batch_size
                df = pd.read_csv(file_name, nrows = batch_size, skiprows= range(1, start_row))

            return "success"
        except:
            return "err"
        
    def display_values(self):
        status = self.process_table()

        if status == 'err':
            raise SyntaxError("Error: Some error occurred while projecting. Please check variables")

