from retrying import retry
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from apiclient.discovery import build


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

    def bq_query_to_table(self, query, table):
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

    # cannot be used at this time, need further generalization in the future
    # def create_random_sample_tb(self, sample_size, from_tb, dest_tb):
    #     random_query = '\
    #     SELECT hash_id \
    #     FROM (SELECT a.hotel_cluster, hash_id, rate*%s AS num, rand() as _rand, row_number() over (partition by a.hotel_cluster order by _rand) AS row_num \
    #     FROM [%s] a INNER JOIN [Expedia.HotelClusterRate] b \
    #     ON a.hotel_cluster == b.hotel_cluster) \
    #     WHERE row_num < num + 1 \
    #     ' % (sample_size, from_tb)
    #     return self.bq_query_to_table(random_query, dest_tb)

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
