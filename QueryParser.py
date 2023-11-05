import re

class QueryParser:

    def __init__(self, query) -> None:
        # get [age, name] from student join athlele on student.id = athlete.id where (cond) groupby col having (cond1) sort [cols] asc/desc.
        self.query = query

    def run_query(self):
        pass