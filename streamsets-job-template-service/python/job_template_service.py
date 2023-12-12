from flask import Flask, request
import job_template_runner
import logging

log_file = '/tmp/streamsets-job-template-service.log'

# Create a logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename=log_file, level=logging.INFO)

app = Flask(__name__)


def validate_request_string_arg(payload, key):
    if not (key in payload.keys() and isinstance(payload[key], str) and len(payload[key]) > 0):
        message = 'Bad value for \'{}\' arg'.format(key)
        logger.error(message)
        raise Exception(message)


def validate_request_list_arg(payload, key):
    if not (key in payload.keys() and isinstance(payload[key], list) and len(payload[key]) > 0):
        message = 'Bad value for \'{}\' arg'.format(key)
        logger.error(message)
        raise Exception(message)


def validate_request_payload(json):
    validate_request_string_arg(json, 'user-id')
    validate_request_string_arg(json, 'user-run-id')
    validate_request_string_arg(json, 'source-type')
    validate_request_string_arg(json, 'target-type')
    validate_request_list_arg(json, 'runtime-parameters')


@app.route('/streamsets/job-template-runner', methods=['POST'])
def handle_job_template_runner_request():
    logger.info('handle_job_template_runner_request')
    try:
        validate_request_payload(request.json)
        print('-- REQUEST PAYLOAD ----------')
        print(request.json)
        print('-----------------------------')
        job_template_runner.run_job_template(request.json)

        return {"status": "OK"}
    except Exception as e:
        return {"status": "There was an error: " + str(e)}


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8888)
