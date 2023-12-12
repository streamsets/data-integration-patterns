insert into streamsets.ingestion_pattern (
  pattern_name,
  source,
  destination,
  create_timestamp
)
 values('http-to-gcs', 'http','gcs', CURRENT_TIMESTAMP);

insert into streamsets.job_template(
  sch_job_template_id,
  delete_after_completion,
  source_runtime_parameters,
  destination_runtime_parameters,
  source_connection_info,
  destination_connection_info,
  create_timestamp
  ) values (
    'c09f728a-2a73-4c7e-b735-2512039a9e6b:8030c2e9-1a39-11ec-a5fe-97c8d4369386',
    false,
    '{"HTTP_MODE": "POLLING", "HTTP_METHOD": "GET"}',
    '{}',
    '{}',
    '{"GCS_CONNECTION" : "9c960db9-7904-47c4-bbc8-4c95dcf9c959:8030c2e9-1a39-11ec-a5fe-97c8d4369386"}',
    CURRENT_TIMESTAMP
);


insert into streamsets.ingestion_pattern_job_template_relationship (
  ingestion_pattern_id,
  job_template_id,
  schedule
) select p.ingestion_pattern_id, t.job_template_id, '{}'
    from  streamsets.ingestion_pattern p,
          streamsets.job_template t
     where p.source =  'http'
     and p.destination = 'gcs'
     and t.sch_job_template_id = 'c09f728a-2a73-4c7e-b735-2512039a9e6b:8030c2e9-1a39-11ec-a5fe-97c8d4369386';
