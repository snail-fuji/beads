from beads.composed import ComposedExecutor
from beads.features import FeatureExtractor
from beads.parser import Parser


def execute(events_df, query_str):
    parser = Parser()
    extractor = FeatureExtractor()
    processed_events_df = extractor.extract(events_df)
    query = parser.parse(query_str)
    composed = ComposedExecutor(processed_events_df)
    result_df = composed.execute(query)

    return {
        'events': processed_events_df,
        'query': query,
        'result': result_df
    }