create schema all_messages;

alter schema all_messages owner to postgres;

create table all_files
(
    file_name varchar(255),
    current integer,
    total integer,
    processing integer default 0,
    newsgroup_name varchar(255),
    hide integer
);

alter table all_files owner to postgres;

create unique index all_files_file_name_uindex
    on all_files (file_name);

create table all_updates
(
    id serial not null
        constraint updates_pk
            primary key,
    groupname text,
    perminute integer default 0,
    tstamp timestamp default CURRENT_TIMESTAMP
);

alter table all_updates owner to postgres;

create unique index updates_id_uindex
    on all_updates (id);

create table all_subjects
(
    id serial not null
        constraint all_subjects_pk
            primary key,
    subject text
);

alter table all_subjects owner to postgres;

create unique index all_subjects_subject_uindex
    on all_subjects (subject);

