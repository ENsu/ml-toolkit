from retrying import retry
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from apiclient.discovery import build
import string


class BQHandler:

    def __init__(self, project_id, credential_path):
        self.credential_path = credential_path
        self.project_id = project_id
        scopes = ['https://www.googleapis.com/auth/bigquery']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            credential_path, scopes)
        http_auth = credentials.authorize(Http())
        bigquery = build('bigquery', 'v2', http=http_auth)
        self.bigquery = bigquery

    def retry_if_job_running(result):
        """Return True if we should retry (in this case when result is 'RUNNING'), False otherwise"""
        return result == 'RUNNING'

    @retry(retry_on_result=retry_if_job_running, wait_fixed=1000, stop_max_attempt_number=250)
    def check_bq_job_status(self, jobId):
        job = self.bigquery.jobs().get(projectId=self.project_id, jobId=jobId).execute()
        # print "job progress: %s" % job['status']['state']
        return job['status']['state']

    def query_to_table(self, query, table):
        dataset = table.split('.')[0]
        table = table.split('.')[1]
        job = self.bigquery.jobs().insert(projectId=self.project_id,
                    body={'projectId': self.project_id,
                           'configuration':{
                             "query": {
                                  "query": query,
                                  "destinationTable": {
                                    "projectId": self.project_id,
                                    "datasetId": dataset,
                                    "tableId": table
                                  },
                             "writeDisposition":"WRITE_TRUNCATE",
                             "createDisposition":"CREATE_IF_NEEDED",
                             "allowLargeResults":"True"
                          }}}).execute()
        job_id_start_with = "%s:" % self.project_id
        self.check_bq_job_status(job['id'].replace(job_id_start_with, ""))
        return job['id']

    def get_df(self, query):
        return pd.read_gbq(query=query, project_id=self.project_id, private_key=self.credential_path, verbose=False)

    def df_to_table(self, df, table):
        return df.to_gbq(destination_table=table, project_id=self.project_id, private_key=self.credential_path)

    def get_tb_resource(self, table):
        datasetId = table.split('.')[0]
        tableId = table.split('.')[1]
        return self.bigquery.tables().get(projectId=self.project_id, datasetId=datasetId, tableId=tableId).execute()

    def get_tb_len(self, table):
        response = self.get_tb_resource(table)
        return response["numRows"]

    def get_tb_columns(self, table):
        response = self.get_tb_resource(table)
        return [f['name'] for f in response['schema']['fields']]

    def create_hash_id(self, origin_tb, target_tb):
        query = 'SELECT HASH(INTEGER(rand()*100000000000000000)) AS hash_id, * FROM [%s]' % origin_tb
        return self.query_to_table(query, target_tb)

    # cannot be used at this time, need further generalization in the future
    def randomly_split_train_valid(self, from_tb, train_index_tb, valid_index_tb, index_col, strat_col, split_rate="0.9"):
        query = '\
            SELECT %s \
            FROM (SELECT %s, a.%s, num*%s AS expect_num, rand() as _rand, row_number() over (partition by a.%s order by _rand) AS row_num \
            FROM [%s] a INNER JOIN (select %s, count(*) as num from [%s] group by %s) b \
            ON a.%s == b.%s) \
            WHERE row_num < expect_num + 1 \
        ' % (index_col, index_col, strat_col, split_rate, strat_col, from_tb, strat_col, from_tb, strat_col, strat_col, strat_col)
        train_tb_job_id = self.query_to_table(query, train_index_tb)
        query = 'SELECT %s FROM [%s] WHERE (%s NOT IN (SELECT %s FROM [%s]))' % (index_col, from_tb, index_col, index_col, train_index_tb)
        valid_tb_job_id = self.query_to_table(query, valid_index_tb)
        return (train_tb_job_id, valid_tb_job_id)

    def make_pivot_count(table, index_col, col_col, target_table, prefix="", rank_limit=0):
        # first get all the possible column value
        query = "select %s, count(*) as count from [%s] group by %s" % (col_col, table, col_col)
        col_val_list = self.get_df(query).sort_values('count', ascending=False)[col_col].tolist()
        if rank_limit > 0:
            col_val_list = col_val_list[:rank_limit]
        col_val_str = ', '.join(['sum(IF(STRING(%s)="%s", 1, 0)) as %s' % (col_col, col_val, prefix+(str(col_val).replace(" ", ""))) for col_val in col_val_list])
        query = "select %s, %s from [%s] group by %s" % (index_col, col_val_str, table, index_col)
        return self.query_to_table(query, target_table)

    def concat_tables(self, table_list, merge_index, target_table):
        alphabetical_list = list(string.ascii_lowercase)
        total_fields = []
        for i, t in enumerate(table_list):
            t_fields_list = self.get_tb_columns(t)
            t_fields_list.remove(merge_index)
            if len(set(total_fields) & set(t_fields_list)) != 0:
                print "find repeated columns in %s, break down" % t
                return None
            t_fields_list = ["%s.%s AS %s" % (alphabetical_list[i], field, field) for field in t_fields_list]
            total_fields = total_fields + t_fields_list
        columns_str = ", ".join(total_fields)
        merge_str = ""
        merge_str = "".join(["inner join %s %s on a.%s == %s.%s " %
                              (t, alphabetical_list[i+1], merge_index, alphabetical_list[i+1], merge_index) for (i, t) in enumerate(table_list[1:])])
        query = """
            select a.%s AS %s, %s
            from %s a %s
        """ % (merge_index, merge_index, columns_str, table_list[0], merge_str)
        return self.query_to_table(query, target_table)

    # cannot be used at this time, need further generalization in the future
    # def create_random_sample_tb(self, sample_size, from_tb, to_tb, ):
    #     random_query = '\
    #     SELECT hash_id \
    #     FROM (SELECT a.hotel_cluster, hash_id, rate*%s AS num, rand() as _rand, row_number() over (partition by a.hotel_cluster order by _rand) AS row_num \
    #     FROM [%s] a INNER JOIN [Expedia.HotelClusterRate] b \
    #     ON a.hotel_cluster == b.hotel_cluster) \
    #     WHERE row_num < num + 1 \
    #     ' % (sample_size, from_tb)
    #     return self.query_to_table(random_query, dest_tb)

    # cannot be used at this time, need further generalization in the future
    # def create_sampled_train_valid_table(self, valid_size, columns):
    #     self.create_random_sample_tb(valid_size, 'Expedia.TrainValid', 'TMP._hash_id')
    #     valid_query = '\
    #     SELECT %s FROM [Expedia.TrainValid] a INNER JOIN [TMP._hash_id] b \
    #     ON a.hash_id == b.hash_id \
    #     ' % ', '.join(columns)
    #     valid = pd.read_gbq(valid_query, project_id=self.project_id, private_key=self.credential_path, verbose=False)
    #     train_query = '\
    #     SELECT %s FROM [Expedia.TrainTrain] WHERE is_booking == True AND user_id IN \
    #     (SELECT user_id FROM [Expedia.TrainValid] a INNER JOIN [TMP._hash_id] b ON a.hash_id == b.hash_id) \
    #     ' % ', '.join(columns)
    #     train = pd.read_gbq(train_query, project_id=self.project_id, private_key=self.credential_path, verbose=False)
    #     return (train, valid)
