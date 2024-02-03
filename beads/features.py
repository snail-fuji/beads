from beads.simple import SimpleExecutor, get_events_index


class FeatureExtractor():
    def __init__(self):
        pass

    def extract(self, events_df):
        query_events_df = events_df.sort_values(['user_id', 'time', 'type']).copy()
        query_events_df['event_order_id'] = query_events_df.groupby(['user_id']).transform('cumcount')
        query_events_df['event_occurence'] = query_events_df.groupby(['user_id', 'type']).transform('cumcount')
        query_events_df['event_index'] = get_events_index(query_events_df['user_id'], query_events_df['event_order_id'])
        query_events_df = query_events_df.set_index('event_index')

        return query_events_df