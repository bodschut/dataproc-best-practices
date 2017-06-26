#!/usr/bin/env python

import argparse

from subprocess import call


def main(project_id,
         zone,
         cluster_name,
         bucket_name,
         num_workers,
         num_preemptible_workers,
         worker_type,
         master_type,
         disk_size,
         timeout):
    init_actions_path = 'gs://my-bucket/path/to/init-actions-script.sh'
    command = 'gcloud dataproc clusters create {0:s} '.format(cluster_name)
    options = []
    options.append('--metadata "JUPYTER_PORT=8124,JUPYTER_CONDA_PACKAGES=numpy:pandas:scikit-learn:matplotlib,INIT_ACTIONS_REPO=https://github.com/username/my-init-actions-fork.git"')
    options.append('--initialization-actions {:s}'.format(init_actions_path))
    if bucket_name:
        options.append('--bucket {:s}'.format(bucket_name))
    options.append('--properties spark:spark.sql.avro.compression.codec=deflate,spark:spark.executorEnv.PYTHONHASHSEED=0,spark:spark.yarn.am.memory=1024m,mapred:avro.mapred.ignore.inputs.without.extension=false')
    options.append('--worker-machine-type={:s}'.format(worker_type))
    options.append('--master-machine-type={:s}'.format(master_type))
    options.append('--worker-boot-disk-size={:d}'.format(disk_size))
    options.append('--num-preemptible-workers={:d}'.format(num_preemptible_workers))
    options.append('--preemptible-worker-boot-disk-size={:d}'.format(disk_size))
    options.append('--labels timeout={:s}'.format(timeout))
    command = command + ' '.join(options)
    print(command)
    call(command, shell=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
    	'cluster_name', help='Name of the cluster to create')
    parser.add_argument(
        '--project_id', help='Project ID you want to access.', default='my-default-project'),
    parser.add_argument(
        '--zone', help='Region to create clusters in', default='europe-west1-d')
    parser.add_argument(
        '--bucket_name', help='gcs bucket name', default=None)
    parser.add_argument(
        '--num_workers', type=int, help='Number of worker nodes to create', default=2)
    parser.add_argument(
        '--num_preemptible_workers', type=int, help='Number of worker nodes to create', default=2)
    parser.add_argument(
        '--worker_type', help='Machine type for the worker nodes', default='n1-standard-4')
    parser.add_argument(
        '--master_type', help='Machine type for the master node', default='n1-standard-4')
    parser.add_argument(
        '--disk_size', help='Physical disk size for the nodes (default is 50Gb)', default=50
    )
    parser.add_argument(
        '--timeout', type=str, help='max. idle time of the cluster in mins', default='t15'
    )

    args = parser.parse_args()
    main(args.project_id,
         args.zone,
         args.cluster_name,
         args.bucket_name,
         args.num_workers,
         args.num_preemptible_workers,
         args.worker_type,
         args.master_type,
         args.disk_size,
         args.timeout)
