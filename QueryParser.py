import re

class QueryParser:

    def __init__(self, query) -> None:
        self.query = query

    def evaluate_query(self):
        pattern = r"get (*) in the table (*) (joining (*) on (*) = (*))? (where (*))? (groupby (*) (having (*))?)? (sortby (*) [asc|desc])?"
        matches = re.match(pattern, self.query)

        if matches:
            return "VALID"
        else:
            print("Invalid Query")        

    def run_query(self):
        pass