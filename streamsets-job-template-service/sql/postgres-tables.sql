create user streamsets with password 'streamsets';

create schema streamsets authorization streamsets;

create table streamsets.job_template(
  job_template_id                     int  primary key generated always as identity,
  sch_job_template_id                 character varying    not null,
  delete_after_completion             boolean              not null,
  source_runtime_parameters           jsonb,
  destination_runtime_parameters      jsonb,
  source_connection_info              jsonb,
  destination_connection_info         jsonb,
  create_timestamp                    timestamp            not null
);

grant all on streamsets.job_template to streamsets;


create table streamsets.ingestion_pattern(
  ingestion_pattern_id              int                  primary key generated always as identity,
  pattern_name                      character varying    not null,
  source                            character varying    not null,
  destination                       character varying    not null,
  create_timestamp                  timestamp            not null
);

grant all on streamsets.ingestion_pattern to streamsets;


create table streamsets.ingestion_pattern_job_template_relationship(
  rel_id                            int                  primary key generated always as identity,
  ingestion_pattern_id              int                  not null,
  job_template_id                   int                  not null,
  schedule                          character varying,
  CONSTRAINT ingestion_pattern_id
      FOREIGN KEY(ingestion_pattern_id)
    REFERENCES streamsets.ingestion_pattern(ingestion_pattern_id),
  CONSTRAINT job_template_id
      FOREIGN KEY(job_template_id)
    REFERENCES streamsets.job_template(job_template_id)
);

grant all on streamsets.ingestion_pattern_job_template_relationship to streamsets;


create table streamsets.job_instance (
  job_instance_id           int  primary key generated always as identity,
  job_run_id                character varying    not null,
  job_template_id           int not null,
  user_id                   character varying    not null,
  user_run_id               character varying    not null,
  engine_id                 character varying    not null,
  pipeline_id               character varying    not null,
  successful_run            boolean    not null,
  input_record_count        int,
  output_record_count       int,
  error_record_count        int,
  error_message             character varying,
  start_time                timestamp,
  finish_time               timestamp,
  CONSTRAINT job_template_id
      FOREIGN KEY(job_template_id)
    REFERENCES streamsets.job_template(job_template_id)
);

grant all on streamsets.job_instance to streamsets;
