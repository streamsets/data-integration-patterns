import psycopg2
from configparser import ConfigParser
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        # Read database connection properties from ../database.ini file
        parser = ConfigParser()
        parser.read('../database.ini')
        self.db_config = parser['postgresql']

    # returns a database connection:
    def get_database_connection(self):
        db_config = self.db_config
        return psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'])

    # Gets SCH Job Template ID for source and destination
    def get_job_template(self, source, destination):
        conn = None
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            sql = """
           select t.job_template_id, 
                  t.sch_job_template_id,
                  t.delete_after_completion,
                  t.source_runtime_parameters,
                  t.destination_runtime_parameters,
                  t.source_connection_info,
                  t.destination_connection_info
             from streamsets.job_template t, 
                streamsets.ingestion_pattern_job_template_relationship r,
                streamsets.ingestion_pattern p
             where p.source = '{}'
                and p.destination = '{}'
                and p.ingestion_pattern_id = r.ingestion_pattern_id
                and t.job_template_id = r.job_template_id
            """.format(source, destination).replace('\n', '')
            print(sql)
            cursor.execute(sql)
            result = cursor.fetchall()
            if result is not None and len(result) != 0:
                row = result[0]
                job_template_info = {
                    'job_template_id': row[0],
                    'sch_job_template_id': row[1],
                    'delete_after_completion': row[2],
                    'source_runtime_parameters': row[3],
                    'destination_runtime_parameters': row[4],
                    'source_connection_info': row[5],
                    'destination_connection_info': row[6]
                }
                return job_template_info
            else:
                print('Error: No  job_template record found for source \'{}\' and destination \'{}\'', format(source, destination))
            return None

        except Exception as e:
            logger.error("Error reading job_template from Postgres " + str(e))

        finally:
            try:
                conn.close()
            except:
                # Swallow any exceptions closing the connection
                pass

    # Inserts a successful Job Metrics record into the database
    def write_job_metrics(self, metrics_data):
        conn = None
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            sql = """
            insert into streamsets.job_instance (
                job_run_id,
                job_template_id,
                user_id,
                user_run_id,
                engine_id,
                pipeline_id,
                successful_run,
                input_record_count,
                output_record_count,
                error_record_count,
                error_message,
                start_time,
                finish_time
            ) values ( 
                \'{}\',{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',{},{},{},\'{}\',\'{}\',\'{}\'
            )
            """.format(
                metrics_data['job_run_id'],
                metrics_data['job_template_id'],
                metrics_data['user_id'],
                metrics_data['user_run_id'],
                metrics_data['engine_id'],
                metrics_data['pipeline_id'],
                metrics_data['successful_run'],
                metrics_data['input_record_count'],
                metrics_data['output_record_count'],
                metrics_data['error_record_count'],
                metrics_data['error_message'],
                metrics_data['start_time'],
                metrics_data['finish_time']
            )
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error("Error writing job_run_metrics to Postgres " + str(e))
        finally:
            try:
                conn.close()
            except:
                pass
