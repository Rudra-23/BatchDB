import re

class QueryParser:
    """
    Query Parser
    ------------
    get table cols

    join
    - yes - run join return cols
    - no - leave

    where
    - yes - run filter return cols
    - no - leave

    groupby 
    - yes - run sort (col) + groupby(col) return cols
    - having
        - yes - run filter return cols
        - no - leave
    - no - leave

    sort
    - yes - run sort return cols
    - no - leave

    Project - run project
    """

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