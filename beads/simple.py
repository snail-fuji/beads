import pandas as pd


def get_events_index(user_ids, event_ids):
    return user_ids.astype(str).values + '_' + event_ids.astype(str).values


class SimpleExecutor():
    query = None
    sequences_df = None
    query_events_df = None

    def __init__(self, query_events_df):
        self.query_events_df = query_events_df

    def _get_events_by_condition(self, conditions):
        condition = " & ".join([f"({c})" for c in conditions])
        
        selected_events = self.query_events_df.query(condition).index
        
        return self.query_events_df.loc[
            selected_events, 
            ['user_id', 'event_order_id']
        ]

    def _merge_new_nodes(self, new_nodes_df, index):
        return self.sequences_df.merge(new_nodes_df, how='inner', on=['user_id']).query(
            f'(event_{index - 1} < event_{index})'
        )

    def _get_pairs_df(self, current_node, target_node):
        sequences_df = self.sequences_df

        events_A_index = get_events_index(sequences_df['user_id'], sequences_df[f'event_{target_node}'])
        events_B_index = get_events_index(sequences_df['user_id'], sequences_df[f'event_{current_node}'])
        events_A_df = self.query_events_df.loc[events_A_index].reset_index(drop=True)
        events_B_df = self.query_events_df.loc[events_B_index].reset_index(drop=True)

        events_A_df.columns = [f'A_{c}' for c in events_A_df.columns]
        events_B_df.columns = [f'B_{c}' for c in events_B_df.columns]
        event_pairs_df = pd.concat([events_A_df, events_B_df], axis=1)
        event_pairs_df.index = sequences_df.index
        
        return event_pairs_df

    def _select_pairs_df(self, event_pairs_df, condition):
        return event_pairs_df.query(condition)

    def _initialize_sequences(self, query):
        self.query = [q for _, q in query]
        self.query_keys = [i for i, _ in query]
        self.sequences_df = self._get_events_by_condition(
            self.query[0]['node_conditions']
        ).reset_index(drop=True).rename(columns={
            'event_order_id': 'event_0'
        })

    def _get_events_mapping(self):
        return {
            f'event_{index}': f'event_{key}'
            for index, key in enumerate(self.query_keys)
        }

    def execute(self, query):
        """
            Returns: user_id, event_0, event_1, ...
        """
        
        self._initialize_sequences(query)

        for index, query_node in enumerate(self.query[1:]):
            print(f"Step {index + 1} initiated")

            new_nodes_df = self._get_events_by_condition(query_node['node_conditions']).rename(columns={
                'event_order_id': f'event_{index + 1}'
            })
            self.sequences_df = self._merge_new_nodes(new_nodes_df, index + 1) 

            for order_condition in query_node.get('order_conditions', []):
                target_node = order_condition['node_used']
                event_pairs_df = self._get_pairs_df(index + 1, target_node)
                event_pairs_df = self._select_pairs_df(event_pairs_df, order_condition['condition'])
                self.sequences_df = self.sequences_df.loc[event_pairs_df.index].copy()

            self.sequences_df = self.sequences_df.drop_duplicates()

            print(f"Step {index + 1} finished")


        events_mapping = self._get_events_mapping()
        return self.sequences_df.rename(columns=events_mapping)