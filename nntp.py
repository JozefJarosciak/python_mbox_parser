import base64
import nntplib
import os
import quopri
import random
import re
import string
from datetime import date
import dateutil.parser
from bs4 import UnicodeDammit
import configuration

today = date.today()
print("** START **")
print("Starting at:", today)


nntp_connection = configuration.nntp_connection

db_cursor = configuration.db_connection.cursor()


response, groups = nntp_connection.list()

print("Total Number of groups:", len(groups))

b = 0
group_name_fin = ""
count_minutes = 0

for x in range(len(groups)):
    groupName = ""
    if ((groups[x].group.startswith("comp.") is True) \
            or groups[x].group.startswith("humanities.") is True \
            or groups[x].group.startswith("microsoft.") is True \
            or groups[x].group.startswith("news.") is True \
            or groups[x].group.startswith("rec.") is True \
            or groups[x].group.startswith("sci.") is True \
            or groups[x].group.startswith("soc.") is True \
            or groups[x].group.startswith("talk.") is True)\
            and ((int(groups[x].last) - int(groups[x].first)) > configuration.syncGroupsOverNumPosts)\
            and "binaries" not in groups[x].group:
        b = b + 1
        groupName = groups[x].group

        file_name = ""
        current_position_in_db = 0
        last_message_count = 0
        is_file_being_processed = 0
        processing_message_counter = 0
        processing_message_counter2 = 0
        group_name_fin = ""

        try:
            sql = f"SELECT * FROM all_messages.__all_files WHERE file_name = '{groupName}' LIMIT 1"
            db_cursor.execute(sql)
            details = db_cursor.fetchone()
            file_name = details[0]
            current_position_in_db = details[1]
            last_message_count = details[2]
            is_file_being_processed = details[3]
            # db_cursor.close()
        except Exception:
            file_name = ""
            current_position_in_db = 0
            last_message_count = 0
            is_file_being_processed = 0
            # print("Exception #: 1")
            # db_cursor.close()

            print(b, groupName, groups[x].first, groups[x].last, current_position_in_db, last_message_count)
        if (file_name == "") or (current_position_in_db == 0 and last_message_count == 0
                                 and is_file_being_processed == 0) or (current_position_in_db < last_message_count
                                                                       and is_file_being_processed == 0):

            group_name_fin_db = groupName.replace(".", "_").replace("-", "_").replace("+", "_")
            if len(group_name_fin_db) > 45:
                group_name_fin_db = group_name_fin_db[-45:]

            try:
                # db_cursor.close()
                sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{groupName}', 0, 0 ,1,'{groupName}') ON CONFLICT (file_name) DO UPDATE SET processing=1"
                # sql = "INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('sci.homebrew.20140221.mbox', 0, 0 ,1,'sci.homebrew') ON CONFLICT (file_name) DO UPDATE SET processing=1"
                db_cursor.execute(sql)
                configuration.db_connection.commit()
                # db_cursor.close()
            except Exception:
                # print("Exception #: 3")
                # db_cursor.close()
                # exit()
                pass

            # Create tables for a new group
            # db_cursor.close()
            # db_cursor = configuration.db_connection.cursor()
            sql = f"select exists(select * from information_schema.tables where table_name='{group_name_fin_db}_headers')"
            db_cursor.execute(sql)
            exist = db_cursor.fetchone()[0]
            # db_cursor.close()

            if not exist:
                try:
                    # db_cursor.close()
                    sql = f"create table all_messages.{group_name_fin_db}_headers(id bigserial not null constraint {group_name_fin_db}_headers_pk primary key, dateparsed timestamp, subj_id bigint, ref smallint, msg_id text, msg_from bigint, enc text, contype text, processed timestamp default CURRENT_TIMESTAMP);alter table all_messages.{group_name_fin_db}_headers owner to postgres;"
                    # db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    # db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_refs(id bigint, ref_msg text default null);alter table all_messages.{group_name_fin_db}_refs owner to postgres;"
                    # db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    # db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_body(id bigint primary key, data text default null);alter table all_messages.{group_name_fin_db}_body owner to postgres;"
                    # db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    # db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_from(id serial not null constraint {group_name_fin_db}_from_pk primary key, data text);alter table all_messages.{group_name_fin_db}_from owner to postgres;"
                    # db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    # db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_subjects(id serial not null constraint {group_name_fin_db}_subjects_pk primary key, subject text);alter table all_messages.{group_name_fin_db}_subjects owner to postgres;"
                    # db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    # db_cursor.close()

                    sql = f"create unique index {group_name_fin_db}_headers_uiidx on all_messages.{group_name_fin_db}_headers(id);" \
                          f"create unique index {group_name_fin_db}_headers_umidx on all_messages.{group_name_fin_db}_headers(msg_id);" \
                          f"create unique index {group_name_fin_db}_body_idx on all_messages.{group_name_fin_db}_body(id);" \
                          f"create unique index {group_name_fin_db}_from_idx on all_messages.{group_name_fin_db}_from(data);" \
                          f"create unique index {group_name_fin_db}_subjects_idx on all_messages.{group_name_fin_db}_subjects(subject);"
                    # db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    # db_cursor.close()
                except Exception:
                    pass


            def groupnum(number):
                s = '%d' % number
                groups = []
                while s and s[-1].isdigit():
                    groups.append(s[-3:])
                    s = s[:-3]
                return s + ','.join(reversed(groups))


            def find_between(s, first, last):
                try:
                    start = s.index(first) + len(first)
                    end = s.index(last, start)
                    return s[start:end]
                except ValueError:
                    return ""


            def removeNonAscii(s):
                return "".join(i for i in s if ord(i) < 126 and ord(i) > 31)


            def clean_string(header_part, encoding):
                orig_header_part = header_part
                header_part = header_part.rstrip(os.linesep).replace("\n", "")
                encoding_quoted = encoding

                if '?q?' in header_part:
                    encoding_quoted = find_between(header_part, '=?', '?')
                    header_part = header_part.split("?q?", 1)[1]  # .replace("_", " ")
                    # header_part = find_between(header_part, 'q?', '?').replace("_", " ")
                elif '?Q?' in header_part:
                    encoding_quoted = find_between(header_part, '=?', '?')
                    header_part = header_part.split("?Q?", 1)[1]  # .replace("_", " ")
                elif '?b?' in header_part:
                    encoding_quoted = find_between(header_part, '=?', '?')
                    header_part = header_part.split("?b?", 1)[1]  # .replace("_", " ")
                    try:
                        header_part = base64.b64decode(header_part)
                    except Exception:
                        try:
                            header_part = base64.b64decode(header_part)
                        except Exception:
                            header_part = orig_header_part
                elif '?B?' in header_part:
                    encoding_quoted = find_between(header_part, '=?', '?')
                    header_part = header_part.split("?B?", 1)[1]  # .replace("_", " ")
                    try:
                        header_part = base64.b64decode(header_part)
                    except Exception:
                        try:
                            header_part = base64.b64decode(header_part)
                        except Exception:
                            header_part = orig_header_part

                if 'unknown' in encoding_quoted:
                    encoding_quoted = encoding
                elif 'x-user-defined' in encoding_quoted:
                    encoding_quoted = encoding

                try:
                    header_part = quopri.decodestring(header_part).decode(encoding_quoted)
                    return header_part
                except Exception:
                    try:
                        header_part = quopri.decodestring(header_part).decode(encoding)
                        return header_part
                    except Exception:
                        try:
                            dammit = UnicodeDammit(header_part)
                            if dammit.original_encoding:
                                header_part = quopri.decodestring(header_part).decode(dammit.original_encoding)
                                return header_part
                            else:
                                header_part = quopri.decodestring(header_part).decode('ascii')
                                return header_part
                        except Exception:
                            try:
                                header_part = quopri.decodestring(header_part).decode("ansi")
                                return header_part
                            except Exception:
                                try:
                                    header_part = header_part.encode('utf8', 'surrogatepass').decode('utf8',
                                                                                                     'surrogatepass')
                                    return header_part
                                except Exception:
                                    return ""


            resp, count, first, last, name = nntp_connection.group(groupName)
            all_count = last
            count_really_inserted = 0
            count_emptybody_inserted = 0

            if first < current_position_in_db:
                first = current_position_in_db
            print('Group', name, 'has', count, 'articles, range', first, 'to', last)


            # if (first+configuration.syncInTiersOfNumPosts) >= all_count:
            #     end = all_count
            # else:
            #     end = first+configuration.syncInTiersOfNumPosts

            end = all_count
            print("Processing: ", first, end)

            try:
                resp, overviews = nntp_connection.over((first, end))
            except Exception as inst:
                print(inst)

            # (5, {'subject': 'Re: Nominate Hirokazu Yamamoto (oceancity) for commit privs.', 'from': 'Jeroen Ruigrok van der Werven <asmodai@in-nomine.org>', 'date': 'Mon, 11 Aug 2008 22:15:34 +0200', 'message-id': '<20080811201534.GL57679@nexus.in-nomine.org>', 'references': '<6167796BFEB5D0438720AC212E89A6B0078F4D64@exchange.onresolve.com> <48A0995D.6010902@v.loewis.de>', ':bytes': '5100', ':lines': '14', 'xref': 'news.gmane.org gmane.comp.python.committers:5'})
            for id, over in overviews:
                processing_message_counter = id
                processing_message_counter2 = processing_message_counter2 + 1

                if processing_message_counter % 1000 == 0:
                    percentage = round(100 * float(processing_message_counter) / float(all_count), 2)

                    # Show how many messsages we're processing per minute
                    # sql_count = "SELECT COUNT(*) FROM all_messages.headers WHERE processed >= (now() - INTERVAL \'1 MINUTE\')"
                    # sql_count = "SELECT COUNT(*) FROM all_messages.headers"
                    try:
                        # db_cursor.close()
                        sql = f"SELECT COUNT(*) FROM all_messages.{group_name_fin_db}_headers WHERE processed >= (now() - INTERVAL '1 MINUTE')"
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        messages_per_minute1 = db_cursor.fetchone()[0]
                        # db_cursor.close()
                    except Exception:
                        # print("Exception #: 5")
                        # db_cursor.close()
                        messages_per_minute1 = 0
                        # exit()

                    try:
                        # db_cursor.close()
                        sql = f"INSERT INTO all_messages.__all_updates(groupname,perminute) VALUES ((%s), (%s))"
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql, (groupName, messages_per_minute1))
                        configuration.db_connection.commit()
                        # db_cursor.close()
                    except Exception as err:
                        # print("Exception #: 6")
                        # db_cursor.close()
                        # exit()
                        pass

                    # Delete all execept last 100 most recent update messages
                    try:
                        # db_cursor.close()
                        sql = f"DELETE FROM all_messages.__all_updates WHERE id <= (SELECT id FROM (SELECT id FROM all_messages.__all_updates ORDER BY id DESC LIMIT 1 OFFSET 10000) foo);"
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        # db_cursor.close()
                    except Exception:
                        # print("Exception #: 7")
                        # db_cursor.close()
                        # exit()
                        pass

                    try:
                        # db_cursor.close()
                        sql = f"select SUM(perminute) from all_messages.__all_updates where id in (SELECT MAX(id) as t FROM all_messages.__all_updates WHERE tstamp >= (now() - INTERVAL '1 MINUTE') group by groupname);"
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        messages_per_minute1 = db_cursor.fetchone()[0]
                        # db_cursor.close()
                        if not messages_per_minute1:
                            messages_per_minute1 = 0
                        print(groupName + ": " + str(processing_message_counter) + " of " + str(
                            all_count) + " (" + str(percentage) + "%) | " + str(
                            groupnum(messages_per_minute1)) + " msgs/min (" + str(
                            groupnum(messages_per_minute1 * 60)) + " hr, " + str(
                            groupnum(messages_per_minute1 * 60 * 24)) + " day, " + str(
                            parsed_date) + " - Added: " + str(count_really_inserted) + " - Empty Body Added: " + str(count_emptybody_inserted))
                    except Exception:
                        # print("Exception #: 8")
                        # db_cursor.close()
                        # exit()
                        pass

                if count_emptybody_inserted>20:
                    print("Empty Body Inserted Count: ", count_emptybody_inserted)
                    exit(0)

                # RESET ALL VARS
                parsed_encoding = "utf-8"
                parsed_content_type = None
                parsed_message_id = None
                parsed_date = None
                parsed_subject = None
                parsed_subject_original = None
                parsed_ref = None
                parsed_body_text = ""
                parsed_body_text_original = None
                parsed_from = None
                parsed_from_original = None
                has_ref = 0

                # Get the rest
                try:
                    parsed_date = nntplib.decode_header(over['date'])
                except Exception:
                    pass

                try:
                    parsed_content_type = nntplib.decode_header(over['content-type'])
                except Exception:
                    pass

                try:
                    parsed_ref = nntplib.decode_header(over['references'])
                except Exception:
                    pass

                try:
                    parsed_subject = nntplib.decode_header(over['subject'])
                except Exception:
                    pass

                try:
                    parsed_subject_original = nntplib.decode_header(over['subject'])
                except Exception:
                    pass

                try:
                    parsed_message_id = nntplib.decode_header(over['message-id'])
                except Exception:
                    pass

                try:
                    parsed_from = nntplib.decode_header(over['from'])
                except Exception:
                    pass

                try:
                    if '(' in parsed_date:
                        parsed_date = parsed_date.split("(")[0].strip()
                    else:
                        parsed_date = parsed_date.strip()
                except Exception:
                    pass

                failing_zones_to_check = ['-13', '-14', '-15', '-16', '-17', '-18', '-19', '-20', '-21', '-22',
                                          '-23', '-24', '+15', '+16', '+17', '+18', '+19', '+20', '+21', '+22',
                                          '+23', '+24']
                try:
                    for failedzone in failing_zones_to_check:
                        if failedzone in parsed_date:
                            parsed_date = parsed_date.split(failedzone)[0]
                            print('Fixed: ' + parsed_date + ' | ' + failedzone)
                            break
                    else:
                        parsed_date = dateutil.parser.parse(parsed_date, tzinfos=configuration.timezone_info)
                except Exception:
                    try:
                        # Try to parse/convert NNTP-Posting-Date
                        parsed_date = nntplib.decode_header(over['NNTP-Posting-Date'])
                        for failedzone in failing_zones_to_check:
                            if failedzone in parsed_date:
                                parsed_date = parsed_date.split(failedzone)[0]
                                print('Fixed NNTP: ' + parsed_date + ' | ' + failedzone)
                                break
                        else:
                            parsed_date = dateutil.parser.parse(parsed_date, tzinfos=configuration.timezone_info)
                    except Exception:
                        # new_headers.append(tuple(("odate", value)))
                        continue

                if parsed_ref:
                    parsed_ref = clean_string(parsed_ref, parsed_encoding)
                else:
                    parsed_ref = ""

                # DATA CLEAN UP - message_id
                if parsed_message_id:
                    parsed_message_id = clean_string(parsed_message_id, parsed_encoding)
                    parsed_message_id = parsed_message_id.replace("'", "")
                    parsed_message_id = parsed_message_id.replace(" ", "").replace('\n', ' ').replace('\r', '')
                else:
                    parsed_message_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

                # DATA CLEAN UP - message_subject
                stringType = None
                if parsed_subject:
                    parsed_subject = clean_string(parsed_subject, parsed_encoding)
                    if '\\u' in ascii(parsed_subject):
                        parsed_subject = ascii(parsed_subject)
                    if len(parsed_subject) > 250:
                        parsed_subject = parsed_subject.split("=?")[0]


                # DATA CLEAN UP - message_subject
                if parsed_from:
                    parsed_from = clean_string(parsed_from, parsed_encoding)

                #############################################
                # ADD MESSAGE DETAILS INTO POSTGRES
                #############################################
                inserted_subject_id = None
                inserted_from_id = None
                inserted_header_id = None
                msg_exist = 0
                try:
                    # Check If MSG ID already in db
                    # db_cursor.close()
                    # db_cursor = configuration.db_connection.cursor()
                    parsed_message_id = removeNonAscii(parsed_message_id)
                    query = f"select id from all_messages.{group_name_fin_db}_headers where msg_id='{parsed_message_id}';"
                    db_cursor.execute(query)
                    inserted_header_id = db_cursor.fetchone()[0]
                    if inserted_header_id:
                        msg_exist = 1
                    else:
                        inserted_header_id = None

                    # db_cursor.close()
                except Exception:
                    # print("Exception #: 9")
                    # print(query)
                    # db_cursor.close()
                    try:
                        # Check If MSG ID already in db
                        # db_cursor = configuration.db_connection.cursor()

                        query = f"select id from all_messages.{group_name_fin_db}_headers where msg_id='{parsed_message_id}'"
                        db_cursor.execute(query)
                        inserted_header_id = db_cursor.fetchone()[0]
                        if inserted_header_id:
                            msg_exist = 1
                        else:
                            inserted_header_id = None
                        # print("message_exists:")
                        # print(msg_exist)
                        # db_cursor.close()
                    except Exception:
                        print("Passing: " + parsed_message_id)
                        # print("Exception #: 10")
                        # db_cursor.close()
                        msg_exist = 0
                    pass

                bodyIsEmpty = 0
                bodyExist = 1
                try:
                    # If Msg exists, let's find if body is empty
                    if msg_exist == 1:
                        query = f"select count(*) from all_messages.{group_name_fin_db}_body where id={inserted_header_id} and data=''"
                        db_cursor.execute(query)
                        bodyIsEmpty = db_cursor.fetchone()[0]
                except Exception:
                    pass

                try:
                    # If Msg exists, let's find if there is any body entry for this message
                    if msg_exist == 1:
                        query = f"select count(*) from all_messages.{group_name_fin_db}_body where id={inserted_header_id}"
                        db_cursor.execute(query)
                        bodyExist = db_cursor.fetchone()[0]
                except Exception:
                    pass

                #if bodyIsEmpty == 1:                   print("Message Exists but has Empty Body: ", parsed_message_id, query)

                #if bodyExist == 0:                     print("Message Exists but Body Not Exists: ", parsed_message_id, query)

                # Continue only if MSG not in the headers db
                if msg_exist == 0 or bodyIsEmpty == 1 or bodyExist == 0:

                    # try:
                    try:
                        # db_cursor.close()
                        # Add a unique subject line
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) RETURNING id"
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql, (parsed_subject, parsed_subject))
                        configuration.db_connection.commit()
                        inserted_subject_id = db_cursor.fetchone()[0]
                        # db_cursor.close()
                    except Exception:
                        # print("Exception #: 11")
                        # db_cursor.close()
                        # exit()
                        if inserted_subject_id is None:
                            try:
                                # db_cursor.close()
                                parsed_subject = parsed_subject.encode("ascii", "ignore").decode()
                                parsed_subject = re.sub(r'[^\x00-\x7f]', r'', parsed_subject)
                                sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) RETURNING id"
                                # db_cursor = configuration.db_connection.cursor()
                                db_cursor.execute(sql, (parsed_subject, parsed_subject))
                                configuration.db_connection.commit()
                                inserted_subject_id = db_cursor.fetchone()[0]
                                # db_cursor.close()
                            except Exception:
                                # print("Exception #: 12")
                                # db_cursor.close()
                                # exit()
                                try:
                                    # db_cursor.close()
                                    parsed_subject = re.sub(r'[^\x00-\x7f]', r'', parsed_subject_original)
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) RETURNING id"
                                    # db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (parsed_subject, parsed_subject))
                                    configuration.db_connection.commit()
                                    inserted_subject_id = db_cursor.fetchone()[0]
                                    # db_cursor.close()
                                except Exception:
                                    # print("Exception #: 13")
                                    # db_cursor.close()
                                    # exit()
                                    pass

                    try:
                        # Add a unique from line
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES ((%s)) ON CONFLICT(data) DO UPDATE SET data=(%s) RETURNING id"
                        #db_cursor = configuration.db_connection.cursor()
                        #print(parsed_from)
                        db_cursor.execute(sql, (parsed_from, parsed_from))
                        configuration.db_connection.commit()
                        inserted_from_id = db_cursor.fetchone()[0]
                    except Exception:
                        # print("Exception #: 13")
                        print(group_name_fin_db, sql)
                        if inserted_from_id is None:
                            try:
                                # db_cursor.close()
                                parsed_from = parsed_from.encode("ascii", "ignore").decode()
                                parsed_from = re.sub(r'[^\x00-\x7f]', r'', parsed_from)
                                # print("Exception #: 14")
                                print(parsed_from)
                                sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES (%s) ON CONFLICT(data) DO UPDATE SET data=(%s) RETURNING id"
                                # db_cursor = configuration.db_connection.cursor()
                                db_cursor.execute(sql, (parsed_from, parsed_from))
                                configuration.db_connection.commit()
                                inserted_from_id = db_cursor.fetchone()[0]
                                # db_cursor.close()
                            except Exception:
                                try:
                                    # db_cursor.close()
                                    parsed_from = re.sub(r'[^\x00-\x7f]', r'', parsed_from_original)
                                    # print("Exception #: 15")
                                    print(parsed_from)
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES (%s) ON CONFLICT(data) DO UPDATE SET data=(%s) RETURNING id"
                                    # db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (parsed_from, parsed_from))
                                    configuration.db_connection.commit()
                                    inserted_from_id = db_cursor.fetchone()[0]
                                    # db_cursor.close()
                                except Exception:
                                    try:
                                        # db_cursor.close()
                                        parsed_from = re.search(r'<(.*?)>', parsed_from).group(1)
                                        # print("Exception #: 16")
                                        #print(parsed_from)
                                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES ('{parsed_from}') ON CONFLICT(data) DO UPDATE SET data=('{parsed_from}') RETURNING id"
                                        #print(sql)
                                        # db_cursor = configuration.db_connection.cursor()
                                        #print("ss1")
                                        db_cursor.execute(sql)
                                        #print("ss2")
                                        configuration.db_connection.commit()
                                        #print("ss3")
                                        inserted_from_id = db_cursor.fetchone()[0]
                                        #print(inserted_from_id)
                                        # db_cursor.close()
                                    except Exception:
                                        # print("Exception #: 17a")
                                        # exit()
                                        pass
                    # Add a header info - pass in the subject line id from the previous statement
                    #
                    if not inserted_from_id:
                       if bodyIsEmpty == 1 or bodyExist == 0:
                           #print("I couldn't get inserted_from_id!")
                           pass
                       else:
                           print("failure - no inserted_from_id", parsed_message_id)
                           exit()


                    if parsed_ref:
                        has_ref = 1
                    else:
                        has_ref = 0


                    if msg_exist == 0:
                        try:
                            # db_cursor.close()
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_headers(dateparsed, subj_id, ref, msg_id, msg_from, enc, contype) VALUES ((%s), (%s), (%s), (%s), (%s), (%s), (%s)) RETURNING id"
                            # db_cursor = configuration.db_connection.cursor()
                            db_cursor.execute(sql, (
                                parsed_date, inserted_subject_id, has_ref, parsed_message_id, inserted_from_id,
                                parsed_encoding,
                                parsed_content_type))
                            configuration.db_connection.commit()
                            inserted_header_id = db_cursor.fetchone()[0]
                            # db_cursor.close()
                        except Exception:
                            # print("Exception #: 16a")
                            # db_cursor.close()
                            # exit()
                            #print('Duplicate MSG ID: ' + parsed_message_id)
                            pass

                            continue

                        if parsed_ref:
                            split_refs = parsed_ref.split(' ')
                            for split in split_refs:
                                try:
                                    # db_cursor.close()
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_refs(id, ref_msg) VALUES ((%s), (%s));"
                                    # db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (inserted_header_id, split.strip()))
                                    configuration.db_connection.commit()
                                    # db_cursor.close()
                                except Exception:
                                    # print("Exception #: 17")
                                    # db_cursor.close()
                                    # exit()
                                    pass

                    try:
                        # Get Body
                        bodyError = 0
                        try:
                            resp, info = nntp_connection.body(over['message-id'])
                            # header = nntp_connection.decode_header(nntp_connection.body(nntplib.decode_header(over['message-id'])))
                            for line in info.lines:
                                dammit = UnicodeDammit(line)
                                parsed_encoding = dammit.original_encoding
                                try:
                                    parsed_body_text += line.decode('utf-8')
                                    parsed_body_text += "\n"
                                except Exception as e:
                                    try:
                                        parsed_body_text += line.decode(parsed_encoding)
                                        parsed_body_text += "\n"
                                    except Exception as e:
                                        parsed_body_text += re.sub(r'[^\x00-\x7f]', r'', line)
                                        parsed_body_text += "\n"
                        except Exception as e:
                            if "430" in str(e):
                                #print(e)
                                pass
                            else:
                                bodyError = 1
                                #print("-------***********ERROR & LINE************------")
                                #print(e)
                                #print(line)
                                pass


                        #if bodyError == 1:
                            #print("-------************MESSAGE ID + FULL BODY***************------")
                            #print(parsed_message_id)
                            #print(parsed_body_text)
                            #print("-------***************************------")
                            #exit(0)

                        # db_cursor.close()
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s)) ON CONFLICT DO NOTHING"
                        # db_cursor = configuration.db_connection.cursor()

                        if len(parsed_body_text) > 0:
                            db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                            configuration.db_connection.commit()
                            # print(inserted_header_id, parsed_message_id, len(parsed_body_text))
                            count_really_inserted = count_really_inserted + 1
                        else:
                            count_emptybody_inserted = count_emptybody_inserted + 1
                            pass
                            #print(f"{inserted_header_id} - NO BODY")
                        # db_cursor.close()
                        # print('Inserted:' + inserted_header_id)
                    except Exception:
                        # print("Exception #: 18")
                        # db_cursor.close()
                        try:
                            parsed_body_text = parsed_body_text.encode("ascii", "ignore").decode()
                            parsed_body_text = re.sub(r'[^\x00-\x7f]', r'', parsed_body_text)
                            # db_cursor.close()
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                            # db_cursor = configuration.db_connection.cursor()
                            db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                            configuration.db_connection.commit()
                            # db_cursor.close()
                        except Exception:
                            # print("Exception #: 19")
                            # db_cursor.close()
                            # parsed_body_text = parsed_body_text_original.encode('utf-8', 'surrogateescape').decode('ANSI')
                            try:
                                # db_cursor.close()
                                parsed_body_text = re.sub(r'[^\x00-\x7f]', r'', parsed_body_text)
                                sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                                # db_cursor = configuration.db_connection.cursor()
                                db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                                configuration.db_connection.commit()
                                # db_cursor.close()
                            except Exception:
                                # print("Exception #: 19")
                                # db_cursor.close()
                                continue

                # except Exception as err:
                #     #print("Exception #: 20")
                #     print("------------------------")
                #     print("-*-" + str(sql) + "-*-")
                #     print("-*-" + str(parsed_message_id) + "-*-")
                #     print("-*-" + str(parsed_date) + "-*-")
                #     print("-*-" + str(parsed_from) + "-*-")
                #     print("-*-" + str(parsed_subject) + "-*-")
                #     print("-*-" + str(parsed_ref) + "-*-")
                #     print("-*-" + str(parsed_encoding) + "-*-")
                #    print("-*-" + str(parsed_content_type) + "-*-")
                #    print("-*-" + str(parsed_body_text) + "-*-")
                #    print("------------------------")
                #     print_psycopg2_exception(err)
                #    print(str(processing_message_counter) + " - " + str(err))
                #    print("------------------------")

                # group_name_fin = file_name
                # update DB - marked file as not being processed anymore
                # print("Final Group Name: " + group_name_fin)
                # group_name_fin = re.sub('\s+',' ',group_name_fin)
                # print("Final Group Name 2: " + group_name_fin)

                if processing_message_counter == all_count:

                    try:
                        # db_cursor.close()
                        sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{groupName}',{processing_message_counter},{last},0,'{groupName}') ON CONFLICT (file_name) DO UPDATE SET current={processing_message_counter}, total={last}, processing=0"
                        # print(sql)
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        # configuration.db_connection.commit()
                        # db_cursor.close()
                    except Exception as err:
                        # print("Exception #: 21")
                        # db_cursor.close()
                        exit()
                else:
                    try:
                        # db_cursor.close()
                        sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{groupName}',{processing_message_counter},{last},1,'{groupName}') ON CONFLICT (file_name) DO UPDATE SET current={processing_message_counter}, total={last}, processing=1"
                        # print(sql)
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        configuration.db_connection.commit()
                        # db_cursor.close()
                    except Exception as err:
                        # print("Exception #: 22")
                        # db_cursor.close()
                        exit()
