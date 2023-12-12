from database_manager import DatabaseManager
from streamsets_manager import StreamSetsManager

import logging
logger = logging.getLogger(__name__)


def run_job_template(request: dict):

    try:

        # Get Database Manager
        db = DatabaseManager()

        # Get StreamSets Manager
        streamsets = StreamSetsManager()

        # Get Job Template Info from the database
        job_template = db.get_job_template(request['source-type'], request['target-type'])

        # Start the Job Template
        job_template_instances = streamsets.run_job_template(job_template, request)

        # Get metrics when Job(s) complete
        streamsets.get_metrics(request['user-id'], request['user-run-id'], job_template, job_template_instances)

    except Exception as e:
        logger.error('Error running Job Template' + str(e))
        raise
