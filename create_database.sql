create schema usenetarchives collate utf8_general_ci;

create table all_files
(
    file_name varchar(255) null,
    current int null,
    total int null,
    processing tinyint(1) unsigned zerofill not null,
    newsgroup_name varchar(255) null,
    hide tinyint(1) null,
    constraint file_name
        unique (file_name)
)
    charset=utf8mb4;

create table from_contacts
(
    id int auto_increment
        primary key,
    from_name varchar(255) null,
    from_email varchar(255) null,
    constraint from_email
        unique (from_email)
)
    charset=utf8mb4;

create index from_contacts_idx_id
    on from_contacts (id);

create table message_body
(
    id int auto_increment
        primary key,
    body mediumtext collate utf8mb4_bin null
)
    charset=utf8mb4;

create index message_body_idx_id
    on message_body (id);

create table message_ids
(
    id int auto_increment
        primary key,
    messageid varchar(255) not null,
    constraint messageid
        unique (messageid)
)
    charset=utf8mb4;

create index message_ids_idx_id
    on message_ids (id);

create index `messageid-index`
    on message_ids (messageid);

create table message_references
(
    messageid int not null,
    reference int not null,
    constraint FK_message_references_message_ids
        foreign key (messageid) references message_ids (id)
            on update cascade on delete cascade,
    constraint FK_message_references_message_ids_2
        foreign key (reference) references message_ids (id)
            on update cascade on delete cascade
)
    charset=utf8mb4;

create index message_references_idx_messageid
    on message_references (messageid);

create index message_references_idx_reference
    on message_references (reference);

create index messageid_reference
    on message_references (messageid, reference);

create table message_subject_lines
(
    id int auto_increment
        primary key,
    subject mediumtext not null
)
    charset=utf8mb4;

create table all_messages
(
    messageid int not null,
    from_contact int not null,
    date_time datetime null,
    has_reference tinyint(1) null,
    subject int not null,
    body int not null,
    processed timestamp default current_timestamp() not null,
    constraint messageid
        unique (messageid, from_contact),
    constraint `FK.from_contact2`
        foreign key (from_contact) references from_contacts (id)
            on update cascade on delete cascade,
    constraint `FK.messageid2`
        foreign key (messageid) references message_ids (id)
            on update cascade on delete cascade,
    constraint FK_all_messages_message_body
        foreign key (body) references message_body (id)
            on update cascade on delete cascade,
    constraint FK_all_messages_message_subject_lines
        foreign key (subject) references message_subject_lines (id)
            on update cascade on delete cascade
)
    charset=utf8mb4;

create index date_time
    on all_messages (date_time);

create index from_contact
    on all_messages (from_contact);

create index has_reference
    on all_messages (has_reference);

create index messageid2
    on all_messages (messageid);

create index processed
    on all_messages (processed);

create index message_subject_line_idx_id
    on message_subject_lines (id);

create table newsgroup_ids
(
    id int auto_increment
        primary key,
    newsgroupname varchar(255) not null,
    constraint newsgroupname
        unique (newsgroupname)
)
    charset=utf8mb4;

create table message_newsgroup_ref
(
    messageid int not null,
    newsgroup int not null,
    constraint messageid_newsgroup_from
        unique (messageid, newsgroup),
    constraint `FK.messageid`
        foreign key (messageid) references message_ids (id)
            on update cascade on delete cascade,
    constraint `FK.newsgroup`
        foreign key (newsgroup) references newsgroup_ids (id)
            on update cascade on delete cascade
)
    charset=utf8mb4;

create index message_newsgroup_re_idx_messageid
    on message_newsgroup_ref (messageid);

create index messageid
    on message_newsgroup_ref (messageid);

create index newsgroup
    on message_newsgroup_ref (newsgroup);

create index newsgroup_ids_idx_id
    on newsgroup_ids (id);

create index `newsgroupname-index`
    on newsgroup_ids (newsgroupname);

