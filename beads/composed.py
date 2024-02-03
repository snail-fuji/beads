import pandas as pd
from beads.simple import SimpleExecutor, get_events_index


def find_intersection(df1, df2):
    return [c for c in df1.columns if c in df2.columns]


def compose_query_logic(query):
    base_query = []
    translated_queries = []
    
    for index, element in query:
        if isinstance(element, dict):  # If the element is a dictionary
            key = next(iter(element))  # Get the 'not', 'and', or 'or' key
            if key == 'not':  # Unary operator
                translated_queries.append({
                    'type': 'not',
                    'query': base_query + element[key]
                })
            elif key in ('and', 'or'):  # 'and' or 'or' n-ary operators
                translated_queries.append({
                    'type': key,
                    'queries': [
                        base_query + sub_element
                        for sub_element in element[key]
                    ]
                })
                    
            else:
                base_query.append((index, element))
        else:  # If the element is not a dictionary, it's a base query component
            base_query += [(index, element)]
    
    # Add the base query at the beginning of the result list
    translated_queries.insert(0, {'type': 'base', 'query': base_query})
    
    return translated_queries


def is_simple_query(query):
    if not isinstance(query, list):
        assert False, f"Wrong query given! {query}"
    for index, element in query:
        if ('node_conditions' not in element):
            return False
    return True


class ComposedExecutor():
    query = None
    query_set = None
    query_events_df = None
    results = None

    def __init__(self, query_events_df):
        self.query_events_df = query_events_df
        self.simple_executor = SimpleExecutor(self.query_events_df)

    def _merge_queries(self, queries):
        base_query = queries[0]['query']
        base_result_df = self._execute_query(base_query)

        for query in queries[1:]:
            if query['type'] == 'not':
                not_result_df = self._execute_query(query['query'])
                events_intersection = find_intersection(base_result_df, not_result_df)
                not_result_df['remove'] = True
                base_result_df = base_result_df.merge(not_result_df, on=events_intersection, how='left')
                base_result_df = base_result_df[base_result_df['remove'] != True].drop(columns='remove').copy()

            if query['type'] == 'and':
                for subquery in query['queries']:
                    and_result_df = self._execute_query(subquery)
                    events_intersection = find_intersection(base_result_df, and_result_df)
                    and_result_df['save'] = True
                    base_result_df = base_result_df.merge(and_result_df, on=events_intersection, how='left')
                    base_result_df = base_result_df[base_result_df['save'] == True].drop(columns='save').copy()

            if query['type'] == 'or':
                for i, subquery in enumerate(query['queries']):
                    or_result_df = self._execute_query(subquery)
                    events_intersection = find_intersection(base_result_df, or_result_df)
                    or_result_df[f'save_{i}'] = 1
                    base_result_df = base_result_df.merge(or_result_df, on=events_intersection, how='left')

                or_fields = [f'save_{i}' for i, _ in enumerate(query['queries'])]
                base_result_df['save'] = base_result_df[or_fields].sum(axis=1)
                base_result_df = base_result_df[base_result_df['save'] > 0].drop(columns=['save'] + or_fields).copy()
        
        return base_result_df


    def _execute_query(self, query):
        simple_executor = self.simple_executor
        if is_simple_query(query):
            return simple_executor.execute(query)
        
        queries = compose_query_logic(query)
        print(queries)
        return self._merge_queries(queries)

    def execute(self, query):
        return self._execute_query(query)
        # self.query = query
        # self.query_set = self._create_query_set()
        # self.results = {}

        # for query_index, simple_query in enumerate(self.query_set):
        #     executor = SimpleExecutor(self.query_events_df)
        #     self.results[query_index] = executor.execute(simple_query['query'])

        # return self._merge_results()