import os
import time
from datetime import timedelta
import logging
import logging.handlers
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging.handlers.transports.sync import SyncTransport
from google.gax.errors import GaxError

#third party libs
from oauth2client.client import GoogleCredentials
from apiclient import discovery

REGION = 'global'

class DataprocDaemon():
    POLL_INTERVAL = 10

    def __init__(self, project_id, logger):
        self.project_id = project_id
        self.dataproc = self._get_client()
        self.cluster_idle_times = {}
        self.logger = logger

    def _get_client(self):
        """Builds an http client authenticated with the service account
        credentials."""
        credentials = GoogleCredentials.get_application_default()
        dataproc = discovery.build('dataproc', 'v1', credentials=credentials)
        return dataproc

    def _get_clusters(self):
        result = self.dataproc.projects().regions().clusters().list(
            projectId = self.project_id,
            region = REGION
        ).execute()
        return result.get('clusters', [])

    def run(self):
        empty_iterations = 0
        while True:
            running_clusters = self._get_clusters()
            if running_clusters:
                empty_iterations = 0
                try:
                    for cluster in running_clusters:
                        cluster_name = cluster['clusterName']
                        cluster_id = cluster['clusterUuid']
                        cluster_timeout = cluster.get('labels', {}).get('timeout', 't15')
                        cluster_timeout = int(cluster_timeout[1:])*60
                        cluster_max_iterations = cluster_timeout/self.POLL_INTERVAL
                        if cluster['status']['state'] == 'DELETING':
                            if cluster_id in self.cluster_idle_times:
                                del self.cluster_idle_times[cluster_id]
                                self.logger.info('Processing manual delete for cluster {}'.format(cluster_name))
                            self.logger.debug('Cluster {} is deleting...'.format(cluster_name))
                            continue
                        if cluster['status']['state'] == 'CREATING':
                            if not cluster_id in self.cluster_idle_times:
                                self.logger.info('New cluster found: {}'.format(cluster_name))
                                self.cluster_idle_times[cluster_id] = 0
                            self.logger.debug('cluster {} is creating...'.format(cluster_name))
                            continue
                        if cluster['status']['state'] == 'UPDATING':
                            if cluster_id in self.cluster_idle_times:
                                self.cluster_idle_times[cluster_id] = 0
                            self.logger.debug('cluster {} is updating...'.format(cluster_name))
                            continue
                        if cluster['status']['state'] == 'ERROR':
                            self.logger.warning('cluster {} has errors... Deleting!'.format(cluster_name))
                            if cluster_id in self.cluster_idle_times:
                                del self.cluster_idle_times[cluster_id]
                            result = self.dataproc.projects().regions().clusters().delete(
                                projectId = self.project_id,
                                region = REGION,
                                clusterName=cluster_name).execute()
                            continue
                        if cluster['status']['state'] == 'RUNNING':
                            if not cluster_id in self.cluster_idle_times:
                                self.logger.info('New cluster found: {}'.format(cluster_name))
                                self.cluster_idle_times[cluster_id] = 0
                            jobs = self.dataproc.projects().regions().jobs().list(
                                projectId = self.project_id,
                                region = REGION,
                                jobStateMatcher = 'ACTIVE',
                                clusterName = cluster_name
                            ).execute()
                            if 'jobs' in jobs:
                                # reset iteration counter, there are active jobs running
                                self.logger.debug(
                                    'Running jobs found for cluster {}'.format(cluster_name)
                                )
                                self.cluster_idle_times[cluster_id] = 0
                            else:
                                # no active jobs, check if allowed idle time has passed
                                current_idle_time = timedelta(
                                    seconds=self.cluster_idle_times[cluster_id]*self.POLL_INTERVAL
                                )
                                if cluster_timeout > 0:
                                    self.logger.debug(
                                        'No running jobs found for cluster {}... Idle time is {} of {} allowed'.format(cluster_name, current_idle_time, timedelta(seconds=cluster_timeout))
                                    )
                                    if cluster_timeout > 0 and self.cluster_idle_times[cluster_id] >= cluster_max_iterations:
                                        # delete cluster
                                        self.logger.warning('Cluster {} was idle for too long... Deleting!'.format(cluster_name))
                                        result = self.dataproc.projects().regions().clusters().delete(
                                            projectId = self.project_id,
                                            region = REGION,
                                            clusterName=cluster_name).execute()
                                        del self.cluster_idle_times[cluster_id]
                                    else:
                                        self.cluster_idle_times[cluster_id] += 1
                                else:
                                    self.logger.debug(
                                        'No running jobs found for cluster {}... Idle time is {}, no auto-delete!'.format(cluster_name, current_idle_time)
                                    )
                                    self.cluster_idle_times[cluster_id] += 1
                except GaxError:
                    continue
            else:
                empty_iterations += 1
                if empty_iterations % 10 == 0:
                    self.logger.debug('No clusters running...')
                    empty_iterations = 0
            time.sleep(self.POLL_INTERVAL)

def main():
    project_id = os.environ.get('GC_PROJECT_ID')
    logger = logging.getLogger("DataprocDaemonLog")
    logger.setLevel(logging.DEBUG)
    client = google.cloud.logging.Client(project=project_id)
    handler = CloudLoggingHandler(client,
                                  name="dataproc-deamon",
                                  transport=SyncTransport)
    logger.addHandler(handler)
    app = DataprocDaemon(project_id, logger)
    app.run()


if __name__ == '__main__':
    main()
