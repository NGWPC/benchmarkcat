import pdb
import pandas as pd
import io

class FlowfileUtils:
    @staticmethod
    def download_flowfiles(bucket_name, flowfile_keys, s3_client):
        dataframes = []
        for flowfile_key in flowfile_keys:
            response = s3_client.get_object(Bucket=bucket_name, Key=flowfile_key)
            flowfile_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(flowfile_content))
            dataframes.append(df)
        return dataframes

    @staticmethod
    def extract_flowstats(flowfile_dfs):
        flowstats_list = []
        for flowfile_df in flowfile_dfs:
            flowstats = {}
            for column in flowfile_df.columns:
                if flowfile_df[column].dtype in ['float64', 'int64']:
                    min_value = flowfile_df[column].min()
                    max_value = flowfile_df[column].max()
                    mean_value = flowfile_df[column].mean()
                    flowstats[column] = {
                        'Min': min_value,
                        'Max': max_value,
                        'Mean': mean_value
                    }
            flowstats_list.append(flowstats)
        return flowstats_list

    @staticmethod
    def create_flowfile_object(flowfile_ids, flowstats_list, columns_list):
        flowfile_objects = {}

        while len(columns_list) < len(flowfile_ids):
            columns_list.append(columns_list[-1])

        for flowfile_id, flowstats, columns in zip(flowfile_ids, flowstats_list, columns_list):
            if 'discharge' in flowstats:
                second_column = 'discharge'
            elif 'streamflow' in flowstats:
                second_column = 'streamflow'
            else:
                raise ValueError("Neither 'discharge' nor 'streamflow' found in DataFrame columns")
            if second_column in flowstats:
                flow_summaries = {
                    "Flowstats": {
                        "discharge": {
                            "Min": float(flowstats[second_column]['Min']),
                            "Max": float(flowstats[second_column]['Max']),
                            "Mean": float(flowstats[second_column]['Mean'])
                        }
                    }
                }

                flowfile_objects[flowfile_id] = {
                    **flow_summaries,
                    "columns": columns
                }
            else:
                raise KeyError(f"Column discharge not found in flowstats")

        return flowfile_objects
