import pandas as pd
import os

class Sort:
    def __init__(self, table_name, attributes, orders):
        self.table_name = table_name
        self.attributes = attributes
        self.orders = orders

        self.orders = [True  if order == 'asc' else False for order in self.orders]

        self.tmp_dir = "./TMP/"
        self.data_dir = "./Data/"

        self.file_name = self.data_dir + table_name + ".csv"
        self.final_file = "_sorted_" + self.table_name

    def split_file(self):
        chunk_size = 500
        reader = pd.read_csv(self.file_name, chunksize=chunk_size)

        for i, chunk in enumerate(reader):
            temp_file_name = self.tmp_dir + f"temp_{i}.csv"
            sorted_chunk = chunk.sort_values(by=self.attributes, ascending = self.orders)
            sorted_chunk.to_csv(temp_file_name, index=False, header=True)

            with open(temp_file_name, 'w', newline="") as file:
                sorted_chunk.to_csv(file, index = False, header = True)

    def merge_files(self):
        temp_file_list = sorted(os.listdir(self.tmp_dir))

        while len(temp_file_list) > 1:
            new_temp_file_list = []

            for i in range(0, len(temp_file_list), 2):
                if i + 1 < len(temp_file_list):
                    file1 = temp_file_list[i]
                    file2 = temp_file_list[i + 1]

                    merged_file = self.merge_two_files(file1, file2)
                    new_temp_file_list.append(merged_file)

                    os.remove(self.tmp_dir + file1)
                    os.remove(self.tmp_dir + file2)
                else:
                    new_temp_file_list.append(temp_file_list[i])
            
            temp_file_list = new_temp_file_list

        os.rename(self.tmp_dir + temp_file_list[0], self.data_dir + self.final_file + '.csv')

    def check_order(self, lhs, rhs, orders):
        for l, r, order in zip(lhs, rhs, orders):
            if order == True:
                if l != r:
                    if type(l) != type(r):
                        return str(l) < str(r)
                    else:
                        return l < r
            else:
                if l != r:
                    if type(l) != type(r):
                        return str(l) > str(r)
                    else:
                        return l > r
        return True

    def merge_two_files(self, file1, file2):

        file1_path = os.path.join(self.tmp_dir, file1)
        file2_path = os.path.join(self.tmp_dir, file2)

        merged_file_name = os.path.splitext(file1)[0] + "_" + os.path.splitext(file2)[0] + ".csv"
        merged_file_path = os.path.join(self.tmp_dir, merged_file_name)

        with open(merged_file_path, 'w', newline="") as merged_file:
            chunksize = 1

            reader1 = pd.read_csv(file1_path, chunksize=chunksize)
            reader2 = pd.read_csv(file2_path, chunksize=chunksize)

            df1 = next(reader1, None)
            df2 = next(reader2, None)

            merged_file.write(",".join(list(df1.columns)) + '\n')
            
            while (df1 is not None) and (df2 is not None):
                lhs = tuple(df1.loc[:, self.attributes].values[0])
                rhs = tuple(df2.loc[:, self.attributes].values[0])
                
                order = self.check_order(lhs, rhs, self.orders)
               
                if order:
                    merged_file.write(df1.to_csv(index=False, header=False))
                    df1 = next(reader1, None)
                else:
                    merged_file.write(df2.to_csv(index=False, header=False))
                    df2 = next(reader2, None)
            
            while df1 is not None:
                merged_file.write(df1.to_csv(index=False, header=False))
                df1 = next(reader1, None)
            
            while df2 is not None:
                merged_file.write(df2.to_csv(index=False, header=False))
                df2 = next(reader2, None)

        return merged_file_name

    def sort_file(self):
        try:
            self.split_file()
            self.merge_files()
        except Exception as e:
            raise SyntaxError("Error: Some error occurred while sorting or joins. Please check variables:" + e)
            