import re
import os

from Operators.Join import Join
from Operators.Filter import Filter
from Operators.Sort import Sort
from Operators.Groupby import Groupby
from Operators.Project import Project

class QueryParser:
    
    def __init__(self, query) -> None:
        self.query = query
        self.data_dir = "./Data/"
        self.tmp_dir = "./TMP/" 

        self.join = True if "joining" in query else False
        self.where = True if "where" in query else False
        self.groupby = True if "groupby" in query else False
        self.having = True if "having" in query else False
        self.sortby = True if "sortby" in query else False

    def evaluate_query(self):
        pattern = r"get \{(.*)\} in the table (\w+)(?: joining (\w+) on ([^\s]+) = ([^\s]+))?(?: where \{([^\{\}]+)\})?(?: groupby ([\.\w]+)(?: having \{([^\{\}]+)\})?)?(?: sortby \{([^\{\}]+)\} (asc|desc))?;"

        matches = re.match(pattern, self.query)

        if matches:
            groups = matches.groups()
            counter = 0

            attributes = {
                "select_cols": True,
                "primary_table": True,
                "secondary_table": self.join,
                "join_table1": self.join,
                "join_table2": self.join,
                "where_cond": self.where,
                "groupby_col": self.groupby,
                "having_cond": self.groupby and self.having,
                "sort_cols": self.sortby,
                "order": self.sortby,
            }

            for attr, condition in attributes.items():
                setattr(self, attr, groups[counter])
                counter += 1

            return "VALID"
        else:
            return "Invalid Query"        


    def get_groupby_cols(self, select_cols):
        groupby_cols = []

        for col in select_cols:
            matches = re.match(r"^(avg|sum|min|max|count)\((.*)\)$", col)
            if matches:
                groupby_cols.append((matches.group(2), matches.group(1)))
        
        return groupby_cols


    def run_query(self):
        table = self.primary_table
        tables = []

        try:
            self.select_cols = [element.strip() for element in self.select_cols.split(',')]
            self.groupby_cols = self.get_groupby_cols(self.select_cols)

            if self.join:
                join_obj = Join(self.primary_table, self.secondary_table, self.join_table1.split('.')[-1], self.join_table2.split('.')[-1])
                table = join_obj.final_file
                tables.append(table)
                join_obj.join_tables()
                
            
            if self.where:
                where_obj = Filter(table, self.where_cond)
                table = where_obj.final_file
                tables.append(table)
                where_obj.filter_data()

            if self.groupby:
                sort_groupby_obj = Sort(table, [self.groupby_col], "asc")
                table = sort_groupby_obj.final_file
                tables.append(table)
                sort_groupby_obj.sort_file()
                

                groupby_obj = Groupby(table, self.groupby_col, self.groupby_cols)
                table = groupby_obj.final_file
                tables.append(table)
                groupby_obj.groupby_table()

                if self.having:
                    having_obj = Filter(table, self.having_cond)
                    table = having_obj.final_file
                    tables.append(table)
                    having_obj.filter_data()

            if self.sortby:
                self.sort_cols = [element.strip() for element in self.sort_cols.split(',')]
                sort_obj = Sort(table, self.sort_cols, self.order)
                table = sort_obj.final_file
                tables.append(table)
                sort_obj.sort_file()

            project_obj = Project(table, self.select_cols)
            project_obj.display_values()
        
            for t in tables:
                os.remove(self.data_dir + t + '.csv')

        except:
            print("Run time error. Please check variables!")
            
            for t in tables:      
                os.unlink(self.data_dir + t + '.csv')
                os.remove(self.data_dir + t + '.csv')
            
