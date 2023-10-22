import pandas as pd
from beads.simple import SimpleExecutor, get_events_index
composed_query_types = ['not']


def merge_queries(base_query, new_query, index):
    return base_query[:index] + new_query + base_query[index:]


class ComposedExecutor():
    query = None
    query_set = None
    query_events_df = None
    results = None

    def __init__(self, events_df):
        query_events_df = events_df.sort_values(['user_id', 'time', 'type']).copy()
        query_events_df['event_order_id'] = query_events_df.groupby(['user_id']).transform('cumcount')
        query_events_df['event_index'] = get_events_index(query_events_df['user_id'], query_events_df['event_order_id'])
        query_events_df = query_events_df.set_index('event_index')
        self.query_events_df = query_events_df

    # def _create_query_set(self):
    #     base_query = [
    #         (query_index, query_node)
    #         for query_index, query_node in enumerate(self.query)
    #         if all([query_type not in query_node for query_type in composed_query_types])
    #     ]
    #     base_query = {
    #         'type': 'base',
    #         'indices': [i for i, _ in base_query]
    #         'query': [q for _, q in base_query]
    #     }

    #     not_queries = []

    #     for query_index, query_node in enumerate(self.query):
    #         if 'not' not in query_node:
    #             continue

    #         new_query = {
    #             'type': 'not',
    #             'indices': base_query['indices'] + [query_index]
    #             'query': merge_queries(
    #                 base_query, 
    #                 query_node['not'], 
    #                 query_index
    #             )
    #         }

    #     # All other queries go here

    #     return [base_query] + not_queries

    # def _merge_results(self):
    #     sequences_df = self.results[0]

    #     for index, results in enumerate(self.results[1:]):
    #         query_type = self.query_set[index + 1]
    #         if query_type == 'not':
    #             pass

    #     return sequences_df

    # def execute(self, query):
    #     self.query = query
    #     self.query_set = self._create_query_set()
    #     self.results = {}

    #     for query_index, simple_query in enumerate(self.query_set):
    #         executor = SimpleExecutor(self.query_events_df)
    #         self.results[query_index] = executor.execute(simple_query['query'])

    #     return self._merge_results()