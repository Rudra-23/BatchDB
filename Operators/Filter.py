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

    def clean_statement(self, namespace, namespace_cleaned):
        for col, col_cleaned in zip(namespace, namespace_cleaned):
            self.statement_cleaned = self.statement_cleaned.replace(col, col_cleaned)

    def filter_data(self):
        with open(self.data_dir + "_filtered_" + self.table_name + '.csv', 'a', newline="") as output:
            
            reader = pd.read_csv(self.data_dir + self.table_name + '.csv', chunksize = 1)

            df = next(reader, None)

            output.write(",".join(list(df.columns)) + '\n')
            
            while df is not None:
                try:
                    namespace = {}
                    for index, item in df.iterrows():
                        namespace.update(item)
                    
                    namespace_cleaned = {key.replace('.', '_').replace('(', '_').replace(')', '_'): value for key, value in namespace.items()}

                    self.clean_statement(list(namespace.keys()), list(namespace_cleaned))
                    
                    if eval(self.statement_cleaned, namespace_cleaned):
                        output.write(df.to_csv(index= False, header=False))    
                except:
                    print("Incorrect syntax or variables.")

                df = next(reader, None)
                

    def filter(self):
        self.filter_data()


if __name__ == "__main__":
    # obj = Filter("_joined_student_athlete", "student.age >= 20 or athlete.sport in ['Basketball', 'Soccer']")
    obj = Filter("_groupby__sorted__filtered__joined_student_athlete", "count(student.id) >= 5 and avg(athlete.weight) != 182.0")
    obj.filter() 