import pandas as pd

class Groupby():

    def __init__(self, table_name, col, aggregations) -> None:
        self.table_name = table_name
        self.col = col
        self.aggregations = aggregations
        self.aggregations_func = aggregations.copy()

        self.tmp_dir = "./TMP/"
        self.data_dir = "./Data/"
        self.final_file = "_groupby_" + self.table_name

        for (k, v) in self.aggregations_func:
            if v == "avg":
                self.aggregations_func.append((k, "sum"))
                self.aggregations_func.append((k, "count")) 

        self.aggregations_func = list(set(self.aggregations_func))

    def groupby_table(self):

        with open(self.data_dir + self.final_file + ".csv", 'a', newline="") as output:
            try:
                reader = pd.read_csv(self.data_dir + self.table_name + ".csv", chunksize = 1)

                cols = [self.col] + [f"{v}({k})" for (k,v) in self.aggregations]
                output.write(",".join(cols) + "\n")
                df = next(reader, None)

                while df is not None and not df.empty:
                    column = df[self.col].values[0]

                    values = {}
                    while df is not None and column == df[self.col].values[0]:
                        for (col, func) in self.aggregations_func:
                            item = f"{func}({col})"
                            if func == "sum":
                                if item in values:
                                    values[item] += df[col].values[0]
                                else:
                                    values[item] = df[col].values[0]
                            elif func == "min":
                                if item in values:
                                    values[item] = min(df[col].values[0], values[item])
                                else:
                                    values[item] = df[col].values[0]
                            elif func == "max":
                                if item in values:
                                    values[item] = max(df[col].values[0], values[item])
                                else:
                                    values[item] = df[col].values[0]
                            elif func == "count":
                                if item in values:
                                    values[item] += 1
                                else:
                                    values[item] = 1

                        df = next(reader, None)

                    for k, v in self.aggregations:
                        if v == "avg":
                            values[f"{v}({k})"] = values[f"sum({k})"] / values[f"count({k})"]
                        

                    arr = [str(values[f"{v}({k})"]) for (k, v) in self.aggregations]
                    arr = [str(column)] + arr  
                    
                    output.write(",".join(arr) + "\n")
            except:
                raise SyntaxError("Error: some error occurred while grouping. Please check variables")

            