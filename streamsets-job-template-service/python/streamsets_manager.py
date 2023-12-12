from configparser import ConfigParser
from streamsets.sdk import ControlHub
from threading import Thread
from time import time, sleep
from datetime import datetime
from database_manager import DatabaseManager
import logging

logger = logging.getLogger(__name__)

# How often to check for updated Job Status
job_status_update_seconds = 10

# How long to wait for a JOb to finish. Jobs that take longer
# than this amount of time will be considered to have failed.
max_wait_time_for_job_seconds = 4 * 60 * 60  # four hours


# noinspection PyMethodMayBeStatic
class StreamSetsManager:
    def __init__(self):
        # Read streamsets connection properties from ../streamsets.ini file
        parser = ConfigParser()
        parser.read('../streamsets.ini')
        self.streamsets_config = parser['streamsets']
        streamsets_config = self.streamsets_config

        # Connect to Control Hub
        self.sch = ControlHub(
            credential_id=streamsets_config['cred_id'],
            token=streamsets_config['cred_token'])

    # Get the static parameters defined in the Job Template table
    def get_static_parameters(self, job_template):
        static_params = {}
        try:
            # Source runtime parameters
            for key in job_template['source_runtime_parameters'].keys():
                static_params[key] = job_template['source_runtime_parameters'][key]

            # Destination runtime parameters
            for key in job_template['destination_runtime_parameters'].keys():
                static_params[key] = job_template['destination_runtime_parameters'][key]

            # Source connection info
            for key in job_template['source_connection_info'].keys():
                static_params[key] = job_template['source_connection_info'][key]

            # Destination connection info
            for key in job_template['destination_connection_info'].keys():
                static_params[key] = job_template['destination_connection_info'][key]

        except Exception as e:
            print('Error getting static parameters: {}'.format(e))
            raise e

        return static_params

    # Club together all the static and dynamic runtime parameters
    def merge_static_and_dynamic_parameters(self, request, job_template):

        # Get the static runtime parameters defined in the template
        static_params = self.get_static_parameters(job_template)

        # Get the runtime params
        runtime_params = request['runtime-parameters']

        # Add the static parameters to each dynamic runtime instance's parameter list
        try:
            for instance in runtime_params:
                for key in static_params:
                    instance[key] = static_params[key]
        except Exception as e:
            print('Error merging static and runtime parameters: {}'.format(e))
            raise e
        print('Consolidated runtime parameters: {}'.format(runtime_params))
        return runtime_params

    # Starts a Job Template and returns a list of Job Template Instances
    def run_job_template(self, job_template, request):

        # Find the Job Template
        job_template_id = job_template['sch_job_template_id']
        try:
            sch_job_template = self.sch.jobs.get(job_id=job_template_id)
            print('Using Job template \'{}\''.format(sch_job_template.job_name))
        except Exception as e:
            logger.error('Error: Job Template with ID \'' + job_template_id + '\' not found.' + str(e))
            raise
        # Merge dynamic and static runtime parameters
        runtime_parameters = self.merge_static_and_dynamic_parameters(request, job_template)

        # Start the Job Template using the runtime parameters in the request
        return self.sch.start_job_template(
            sch_job_template,
            runtime_parameters=runtime_parameters,
            instance_name_suffix='TIME_STAMP',
            attach_to_template=True,
            delete_after_completion=job_template['delete_after_completion'])

    # Get metrics for all Job Template Instances once they complete
    def get_metrics(self, user_id, user_run_id, job_template, job_template_instances):
        for job in job_template_instances:
            # Track each Job Template Instance in a separate thread to avoid blocking
            thread = Thread(target=self.wait_for_job_completion_and_get_metrics,
                            args=(user_id, user_run_id, job_template, job,))
            thread.start()

    # Waits for Job to complete before getting its metrics
    def wait_for_job_completion_and_get_metrics(self, user_id, user_run_id, job_template, job):
        start_seconds = time()
        elapsed_seconds = 0
        while elapsed_seconds < max_wait_time_for_job_seconds:
            elapsed_seconds = time() - start_seconds
            job.refresh()
            if job.status.status == 'INACTIVE' or job.status.status == 'INACTIVE_ERROR':
                break
            sleep(job_status_update_seconds)

        self.write_metrics_for_job(user_id, user_run_id, job_template, job)

    def write_metrics_for_job(self, user_id, user_run_id, job_template_info, job):

        metrics_data = {}
        job.refresh()
        metrics = job.metrics[0]
        history = job.history[0]

        # If job status color is RED, don't consider the Job as successful
        if job.status.status == 'INACTIVE' and history.color == 'GRAY':
            metrics_data['successful_run'] = True
        else:
            metrics_data['successful_run'] = False

        if history.error_message is None:
            metrics_data['error_message'] = ''
        else:
            metrics_data['error_message'] = history.error_message

        metrics_data['user_id'] = user_id
        metrics_data['user_run_id'] = user_run_id
        metrics_data['job_run_id'] = job.job_id
        metrics_data['job_template_id'] = job_template_info['job_template_id']
        metrics_data['engine_id'] = metrics.sdc_id
        metrics_data['pipeline_id'] = job.pipeline_id
        metrics_data['input_record_count'] = metrics.input_count
        metrics_data['output_record_count'] = metrics.output_count
        metrics_data['error_record_count'] = metrics.total_error_count
        metrics_data['start_time'] = datetime.fromtimestamp(history.start_time / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        metrics_data['finish_time'] = datetime.fromtimestamp(history.finish_time / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

        DatabaseManager().write_job_metrics(metrics_data)
