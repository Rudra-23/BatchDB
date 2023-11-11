import re
import pandas as pd
import tabulate

class Filter():
    def __init__(self, table_name, statement) -> None:
        self.table_name = table_name
        self.statement = True if statement == "" else statement
        self.statement_cleaned = self.statement

        self.tmp_dir = "./TMP/"
        self.data_dir = "./Data/"
        
        self.final_file = "_filtered_" + self.table_name

    def clean_statement(self, namespace, namespace_cleaned):
        for col, col_cleaned in zip(namespace, namespace_cleaned):
            self.statement_cleaned = self.statement_cleaned.replace(col, col_cleaned)

    def filter_data(self):
        
        with open(self.data_dir + self.final_file + '.csv', 'a', newline="") as output:
            try:
                reader = pd.read_csv(self.data_dir + self.table_name + '.csv', chunksize = 1)

                df = next(reader, None)

                output.write(",".join(list(df.columns)) + '\n')
                
                while df is not None and not df.empty:
                    try:
                        namespace = {}
                        for index, item in df.iterrows():
                            namespace.update(item)
                        
                        namespace_cleaned = {key.replace('.', '_').replace('(', '_').replace(')', '_'): value for key, value in namespace.items()}

                        self.clean_statement(list(namespace.keys()), list(namespace_cleaned))
                        
                        if namespace_cleaned != {} and eval(self.statement_cleaned, namespace_cleaned):
                            output.write(df.to_csv(index= False, header=False))    
                    except:
                        raise SyntaxError("Please check filter conditions {where and having}.")

                    df = next(reader, None)
            except:
                raise SyntaxError("Error: Some error occurred while filtering. Please check variables inside where and having")
        
