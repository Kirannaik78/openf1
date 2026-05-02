create schema openf1;

drop table openf1.meeting;
drop table openf1.circuit;
drop table openf1.country;


create table if not exists openf1.circuit(
circuit_key bigint primary key,
circuit_short_name varchar(255),
circuit_type varchar(255),
circuit_info_url varchar(255),
circuit_image varchar(255),
created_at timestamp  default CURRENT_TIMESTAMP,
updated_at timestamp default CURRENT_TIMESTAMP 
);

create table if not exists openf1.country(
country_key int primary key,
country_name varchar(255),
country_flag varchar(255),
country_code varchar(50),
created_at timestamp  default CURRENT_TIMESTAMP,
updated_at timestamp default CURRENT_TIMESTAMP 
);

create table if not exists openf1.meeting(
meeting_key bigint primary key,
meeting_name varchar(255),
meeting_official_name varchar(255),
year int,
location varchar(255),
is_cancelled bit,
gmt_offset varchar(255),
date_start timestamp ,
date_end timestamp,
country_key int REFERENCES openf1.country(country_key),
circuit_key bigint REFERENCES openf1.circuit(circuit_key),
created_at timestamp  default CURRENT_TIMESTAMP,
updated_at timestamp default CURRENT_TIMESTAMP 
);


create table if not exists openf1.sessions(
session_key bigint primary key,
session_name varchar(255),
session_type varchar(255),
year int,
date_start timestamp,
date_end timestamp,
is_cancelled bit,
meeting_key bigint REFERENCES  openf1.meeting(meeting_key),
country_key int REFERENCES openf1.country(country_key),
circuit_key bigint references openf1.circuit(circuit_key),
created_at timestamp  default CURRENT_TIMESTAMP,
updated_at timestamp default CURRENT_TIMESTAMP 
);

create table if not exists openf1.drivers(
driver_number int primary key,
first_name varchar(255),
full_name varchar(255),
last_name varchar(255),
broadcast_name varchar(255),
name_acronym varchar(50),
team_name varchar(255),
team_colour varchar(255),
meeting_key bigint REFERENCES openf1.meeting(meeting_key),
session_key bigint REFERENCES openf1.sessions(session_key),
created_at timestamp  default CURRENT_TIMESTAMP,
updated_at timestamp default CURRENT_TIMESTAMP 
);