import re
import pandas as pd
import tabulate

class Filter():
    def __init__(self, table_name, statement) -> None:
        self.table_name = table_name
        self.statement = True if statement == "" else statement


    def filter_data(self):
        with open("./Data/" + "_filtered_" + self.table_name + '.csv', 'a', newline="") as output:
            
            reader = pd.read_csv("./Data/" + self.table_name + '.csv', chunksize = 1)

            df = next(reader, None)

            output.write(",".join(list(df.columns)) + '\n')
            
            while df is not None:
                try:
                    namespace = {}
                    for index, item in df.iterrows():
                        namespace.update(item)
                    
                    namespace = {key.replace('.', '__'): value for key, value in namespace.items()}

                    if eval(self.statement, namespace):
                        output.write(df.to_csv(index= False, header=False))    
                except:
                    print("Incorrect syntax or variables.")

                df = next(reader, None)
                

    def filter(self):
        self.filter_data()


if __name__ == "__main__":
    obj = Filter("_sorted__joined_student_athlete", "student__age <= 20 and athlete__sport in ['Basketball', 'Soccer']")
    obj.filter() 