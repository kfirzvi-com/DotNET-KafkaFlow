-- Runs once, as APP_USER (processor), the first time the Oracle container initializes.
-- Creates the data-type settings table the processor reads at startup and on every refresh.
--
-- Rows are scoped by DOMAIN_NAME so one table serves every domain: a `posts` deployment only ever
-- sees posts rows, so a profiles data type can never switch a posts processor on.

CREATE TABLE DATA_TYPE_SETTINGS (
  DOMAIN_NAME  VARCHAR2(64)  NOT NULL,
  DATA_TYPE_ID VARCHAR2(128) NOT NULL,
  IS_ACTIVE    NUMBER(1)     DEFAULT 0 NOT NULL,
  CONSTRAINT PK_DATA_TYPE_SETTINGS PRIMARY KEY (DOMAIN_NAME, DATA_TYPE_ID),
  CONSTRAINT CK_DATA_TYPE_SETTINGS_ACTIVE CHECK (IS_ACTIVE IN (0, 1))
);

-- Seed data for local runs. Only active (IS_ACTIVE = 1) data types flow through the pipeline;
-- everything else is filtered and counted under messages_filtered_total{reason="inactive_data_type"}.
INSERT INTO DATA_TYPE_SETTINGS (DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE) VALUES ('posts',    'engagement',        1);
INSERT INTO DATA_TYPE_SETTINGS (DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE) VALUES ('posts',    'retired-feed',      0);
INSERT INTO DATA_TYPE_SETTINGS (DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE) VALUES ('profiles', 'directory',         1);
INSERT INTO DATA_TYPE_SETTINGS (DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE) VALUES ('profiles', 'retired-directory', 0);

COMMIT;
