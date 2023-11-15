import pandas as pd

class SortMergeJoin():
    def __init__(self, table1, table2, col1, col2) -> None:
        self.table1 = table1
        self.table2 = table2
        
        
        self.col1 = col1
        self.col2 = col2
        
        self.data_dir = "./Data/"
        self.tmp_dir = "./TMP/"

        self.final_file = "_joined_"+  self.table1 + "_" + self.table2 

    def join_tables(self):
        col1 = self.col1
        col2 = self.col2
        header = False

        with open(self.data_dir + self.final_file + '.csv', 'a', newline="") as output:
            try:        
                chunk_size = 100
                for df1_chunk in pd.read_csv(self.data_dir + self.table1 + ".csv", chunksize=chunk_size):
                    for df2_chunk in pd.read_csv(self.data_dir + self.table2 + ".csv", chunksize=chunk_size):

                        if header != True:
                            header = True
                            list1 = [f"{self.table1.replace('_sorted_', '')}.{c1}" for c1 in list(df1_chunk.columns)]
                            list2 = [f"{self.table2.replace('_sorted_', '')}.{c2}" for c2 in list(df2_chunk.columns)]

                            output.write(",".join(list1 + list2) + '\n')

                        i = j = 0
                        while i < len(df1_chunk) and j < len(df2_chunk):
                            if df1_chunk.iloc[i][col1] < df2_chunk.iloc[j][col2]:
                                i += 1
                            elif df1_chunk.iloc[i][col1] > df2_chunk.iloc[j][col2]:
                                j += 1
                            else:
                                temp = []
                                for v in df1_chunk.iloc[i].values:
                                    temp.append(str(v))
                                for v in df2_chunk.iloc[j].values:
                                    temp.append(str(v))
                                output.write(",".join(temp) + "\n")
                                j += 1

            except Exception as e:
                raise SyntaxError(f"Error: Some error occurred with joining. Please check variables")