import pandas as pd

class Join():
    def __init__(self, table1, table2, col1, col2) -> None:
        self.table1 = table1
        self.table2 = table2
        
        
        self.col1 = col1
        self.col2 = col2
        
        self.data_dir = "./Data/"
        self.tmp_dir = "./TMP/"

        self.final_file = "_joined_"+  self.table1 + "_" + self.table2 + "_" + self.col1

    def join_tables(self):
        col1 = self.col1
        col2 = self.col2
        header = False

        with open(self.data_dir + self.final_file + '.csv', 'a', newline="") as output:
            try:        
                chunk_size = 100

                s1 = 0
                e1 = chunk_size
                
                df1_chunk = pd.read_csv(self.data_dir + self.table1 + ".csv", nrows = chunk_size, skiprows = range(1, s1))
                while not df1_chunk.empty:
                    s2 = 0
                    e2 = chunk_size
                    df2_chunk = pd.read_csv(self.data_dir + self.table2 + ".csv", nrows = chunk_size, skiprows= range(1, s2))

                    while not df2_chunk.empty:

                        if header != True:
                            header = True
                            
                            list1 = [f"{self.table1.replace('_sorted_', '')}.{c1}" for c1 in list(df1_chunk.columns)]
                            list2 = [f"{self.table2.replace('_sorted_', '')}.{c2}" for c2 in list(df2_chunk.columns)]

                            output.write(",".join(list1 + list2) + '\n')
                        
                        
                        if (df1_chunk.iloc[len(df1_chunk)-1][col1] < df2_chunk.iloc[0][col2]):
                            pass 
                        elif (df1_chunk.iloc[0][col1] > df2_chunk.iloc[len(df2_chunk)-1][col2]):
                            pass
                        else:
                            for i in range(0, len(df1_chunk)):
                                for j in range(0, len(df2_chunk)):
                                    
                                    if df1_chunk.iloc[i][col1] == df2_chunk.iloc[j][col2]:
                                        temp = []
                                        for v in df1_chunk.iloc[i].values:
                                            temp.append(str(v))
                                        for v in df2_chunk.iloc[j].values:
                                            temp.append(str(v))
                                        output.write(",".join(temp) + "\n")
                        
                        s2 = e2 + 1
                        e2 += chunk_size
                        df2_chunk = pd.read_csv(self.data_dir + self.table2 + ".csv", nrows = chunk_size, skiprows= range(1, s2))

                    s1 = e1 + 1
                    e1 += chunk_size
                    df1_chunk = pd.read_csv(self.data_dir + self.table1 + ".csv", nrows = chunk_size, skiprows = range(1, s1))


            except Exception as e:
                raise SyntaxError(f"Error: Some error occurred with joining. Please check variables")
            
