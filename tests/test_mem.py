import pandas as pd
from tabulate import tabulate

class Project:
    def __init__(self, table_name, cols) -> None:
        self.table_name = table_name
        self.cols = cols

        self.tmp_dir = "./TMP/"
        self.data_dir = "./Data/"

    def test_load_entire_data(self):
        try:
            file_name =  self.data_dir + f"{self.table_name}.csv"

            df = pd.read_csv(file_name)
            self.cols = self.cols if self.cols != [''] else list(df.columns)
            table = tabulate(df, headers= list(self.cols), tablefmt="fancy_grid")
            
            print(table)

            return "success"
        except:
            return "err"
        
    def test_load_in_chunks(self):
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
        
    def display_values(self, test: bool):
        status = None

        if test == True:
            status = self.test_load_entire_data()
        else:
            status = self.test_load_in_chunks()

        if status == 'err':
            raise SyntaxError("Error: Some error occurred while projecting. Please check variables")


if __name__ == "__main__":
    
    while True:
        request = input("Enter [LoadAll or LoadChunks] or [exit]: ")
        if request == 'exit':
            break
        
        obj = Project("large", [""])
        
        if request == "LoadAll":
            obj.display_values(True)
        else:
            obj.display_values(False)
