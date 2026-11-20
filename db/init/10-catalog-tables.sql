-- Tables owned by gap-td-devices-catalog that the gap service's aggregation views reference.
-- Copied from that repo's Liquibase migrations (20260306, 20260421, 20260424, 20260425).
CREATE SCHEMA IF NOT EXISTS traffic_data;
SET search_path TO traffic_data;


-- Basic Configuration Devices
-- Creates detection site and traffic data sensor entities with CDC columns
-- used for synchronizing configuration data from the configuration service.

-- Detection Site entity (parent level)
CREATE TABLE IF NOT EXISTS detection_sites
(
    id                           VARCHAR(255) NOT NULL PRIMARY KEY,
    uuid                         VARCHAR(36),
    name                         VARCHAR(255) NOT NULL,
    description                  TEXT,
    highway                      VARCHAR(50),
    direction                    VARCHAR(50),
    kilometer                    VARCHAR(50),
    traffic_control_center_id    VARCHAR(50),
    traffic_sub_center_id        VARCHAR(50),
    communication_controller_id  VARCHAR(50),
    lcl_section                  VARCHAR(50),
    asb_section                  VARCHAR(50),
    road_type                    SMALLINT,
    device_state                        SMALLINT,
    sensor_count                 INTEGER
    );


-- Traffic Data Sensor entity (child level, belongs to detection site)
CREATE TABLE IF NOT EXISTS traffic_data_sensors
(
    id                           VARCHAR(255) NOT NULL PRIMARY KEY,
    uuid                         VARCHAR(36),
    name                         VARCHAR(255) NOT NULL,
    description                  TEXT,
    detection_site_id            VARCHAR(255) NOT NULL,
    lane_position                VARCHAR(50),
    orientation                  VARCHAR(50),
    outstation_id                VARCHAR(50),
    traffic_control_center_id    VARCHAR(50),
    traffic_sub_center_id        VARCHAR(50),
    communication_controller_id  VARCHAR(50),
    road_type                    SMALLINT,
    device_state                 SMALLINT
    );

CREATE INDEX IF NOT EXISTS idx_traffic_data_sensors_detection_site_id
    ON traffic_data_sensors (detection_site_id);


-- liquibase formatted sql

-- changeset gap-td-devices-catalog-service:20260421__virtual_detection_sites.sql
CREATE TABLE IF NOT EXISTS virtual_detection_sites
(
    id                          VARCHAR(36)  NOT NULL PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL,
    source_type                 VARCHAR(20)  NOT NULL DEFAULT 'VIRTUAL',
    traffic_control_center_id   VARCHAR(36),
    traffic_sub_center_id       VARCHAR(36),
    communication_controller_id VARCHAR(36),
    outstation_id               VARCHAR(36),
    location_sensor_id          VARCHAR(36),
    highway                     VARCHAR(255),
    lcl_section                 VARCHAR(50),
    device_state                SMALLINT NOT NULL DEFAULT 1,
    direction                   VARCHAR(255),
    road_type                   SMALLINT,
    kilometer                   VARCHAR(255),
    sensor_count                SMALLINT NOT NULL DEFAULT 0,
    created_by                  VARCHAR(36)  NOT NULL,
    updated_by                  VARCHAR(36)  NOT NULL,
    created_at                  TIMESTAMPTZ  NOT NULL,
    updated_at                  TIMESTAMPTZ  NOT NULL,
    deleted_at                  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_virtual_detection_sites_traffic_sub_center ON virtual_detection_sites (traffic_sub_center_id);
CREATE INDEX IF NOT EXISTS idx_virtual_detection_sites_comm_controller ON virtual_detection_sites (communication_controller_id);
CREATE INDEX IF NOT EXISTS idx_virtual_detection_sites_highway ON virtual_detection_sites (highway);
CREATE INDEX IF NOT EXISTS idx_virtual_detection_sites_lcl_section ON virtual_detection_sites (lcl_section);


CREATE TABLE IF NOT EXISTS virtual_detection_site_sensors
(
    virtual_detection_site_id VARCHAR(36) NOT NULL REFERENCES virtual_detection_sites (id) ON DELETE CASCADE,
    sensor_id                 VARCHAR(36) NOT NULL,
    PRIMARY KEY (virtual_detection_site_id, sensor_id)
);

CREATE INDEX IF NOT EXISTS idx_virtual_detection_site_sensors_sensor_id ON virtual_detection_site_sensors (sensor_id);

CREATE TABLE IF NOT EXISTS virtual_detection_site_locations
(
    id                VARCHAR(36) NOT NULL PRIMARY KEY REFERENCES virtual_detection_sites (id) ON DELETE CASCADE,
    geo                         JSONB,
    alert_c                     JSONB,
    asb                         JSONB,
    display                     JSONB,
    kilometer                   JSONB,
    openlr                      JSONB,
    net                         JSONB,
    road_meter                  JSONB,
    text                        JSONB,
    route                       JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- rollback DROP TABLE IF EXISTS virtual_detection_site_locations;
-- rollback DROP TABLE IF EXISTS virtual_detection_site_sensors;
-- rollback DROP TABLE IF EXISTS virtual_detection_sites;
-- liquibase formatted sql

-- changeset gap-td-devices-catalog-service:20260424__virtual_detection_sites_add_asb_section.sql
ALTER TABLE virtual_detection_sites
    ADD COLUMN IF NOT EXISTS asb_section VARCHAR(255);

-- rollback ALTER TABLE virtual_detection_sites DROP COLUMN IF EXISTS asb_section;
-- liquibase formatted sql

-- changeset gap-td-devices-catalog-service:20260425__virtual_detection_sites_add_description.sql
ALTER TABLE virtual_detection_sites
    ADD COLUMN IF NOT EXISTS description TEXT;

-- rollback ALTER TABLE virtual_detection_sites DROP COLUMN IF EXISTS description;
