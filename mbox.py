import base64
import email
import glob
import gzip
import mailbox as mailbox
import ntpath
import os
import quopri
import random
import re
import shutil
import string
import sys
import time
from datetime import date
from email import policy

import cchardet
import dateutil.parser
# START - CONFIGURATION
from bs4 import UnicodeDammit

import configuration

# Wait random time 15-120 seconds before starting
# sleep_time = random.randint(1, 120)
# print("Waiting " + str(sleep_time) + " seconds before start!")
# time.sleep(sleep_time)

where2unzip = ""
print("Connecting PostgreSQL DB")

today = date.today()
print("Starting at:", today)


def print_psycopg2_exception(err2):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_num = traceback.tb_lineno


# db_cursor.execute('SET GLOBAL max_allowed_packet=67108864')
# db_cursor.execute('SET GLOBAL max_connections = 500')
# db_cursor.execute('SET GLOBAL max_delayed_threads = 50')
# db_cursor.execute('SET GLOBAL innodb_io_capacity = 5000')


def convert_encoding(data, new_coding='UTF-8'):
    encoding = cchardet.detect(data)['encoding']
    if new_coding.upper() != encoding.upper():
        data = data.decode(encoding, data).encode(new_coding)
    return data


group_name_fin = ""
count_minutes = 0
# path = r"C:\tmp"

path = configuration.path.replace("\\", "/") + "/"

# END - CONFIGURATION

# Connect to MySQL


# Start processing MBOX files
print("** START **" + str(path))
print("Processing all files on path: " + str(path))

# foo()

files = [f for f in glob.glob(path + '**/*.mbox.gz', recursive=True)]
count = 0
for f in files:
    f = f.replace("\\", "/")
    # filename = f.replace(path, "").replace(".gz", "")
    filename = ntpath.basename(f).replace(".gz", "")

    file_name = ""
    current_position_in_db = 0
    last_message_count = 0
    is_file_being_processed = 0
    processing_message_counter = 0
    group_name_fin = ""
    try:
        sql = f"SELECT * FROM all_messages.__all_files WHERE file_name = '{filename}' LIMIT 1"
        db_cursor = configuration.db_connection.cursor()
        db_cursor.execute(sql)
        details = db_cursor.fetchone()
        file_name = details[0]
        current_position_in_db = details[1]
        last_message_count = details[2]
        is_file_being_processed = details[3]
        db_cursor.close()
    except Exception:
        file_name = ""
        current_position_in_db = 0
        last_message_count = 0
        is_file_being_processed = 0
        print("Exception #: 1")

    if (current_position_in_db > 0) and (current_position_in_db == last_message_count):
        # Move a file from the directory d1 to d2
        try:
            shutil.move(f, configuration.processed_path + filename + '.gz')
            print("Moving File: " + filename + ".gz")
        except Exception:
            print("Exception #: 2")
            pass

    if (file_name == "") or (
            current_position_in_db == 0 and last_message_count == 0 and is_file_being_processed == 0) or (
            current_position_in_db < last_message_count and is_file_being_processed == 0):
        try:
            filename_extract = filename.replace(".mbox", "")
            group_name_fin = filename_extract.replace("." + filename_extract.split(".")[-1], "")
            group_name_fin_db = group_name_fin.replace(".", "_").replace("-", "_").replace("+", "")

            try:
                sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}', 0, 0 ,1,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET processing=1"
                # sql = "INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('sci.homebrew.20140221.mbox', 0, 0 ,1,'sci.homebrew') ON CONFLICT (file_name) DO UPDATE SET processing=1"
                db_cursor = configuration.db_connection.cursor()
                db_cursor.execute(sql)
                configuration.db_connection.commit()
                db_cursor.close()
            except Exception:
                print("Exception #: 3")
                exit()


            # Create tables for a new group
            db_cursor = configuration.db_connection.cursor()
            db_cursor.execute(
                f"select exists(select * from information_schema.tables where table_name='{group_name_fin_db}_headers')")
            exist = db_cursor.fetchone()[0]
            db_cursor.close()

            if not exist:
                try:
                    sql = f"create table all_messages.{group_name_fin_db}_headers(id bigserial not null constraint {group_name_fin_db}_headers_pk primary key, dateparsed timestamp, subj_id bigint, ref smallint, msg_id text, msg_from bigint, enc text, contype text, processed timestamp default CURRENT_TIMESTAMP);alter table all_messages.{group_name_fin_db}_headers owner to postgres;"
                    db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_refs(id bigint, ref_msg text default null);alter table all_messages.{group_name_fin_db}_refs owner to postgres;"
                    db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_body(id bigint primary key, data text default null);alter table all_messages.{group_name_fin_db}_body owner to postgres;"
                    db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_from(id serial not null constraint {group_name_fin_db}_from_pk primary key, data text);alter table all_messages.{group_name_fin_db}_from owner to postgres;"
                    db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_subjects(id serial not null constraint {group_name_fin_db}_subjects_pk primary key, subject text);alter table all_messages.{group_name_fin_db}_subjects owner to postgres;"
                    db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    db_cursor.close()

                    sql = f"create unique index {group_name_fin_db}_headers_id_uindex on all_messages.{group_name_fin_db}_headers(id);" \
                          f"create unique index {group_name_fin_db}_headers_id_umsgid on all_messages.{group_name_fin_db}_headers(msg_id);" \
                          f"create unique index {group_name_fin_db}_body_id_uindex on all_messages.{group_name_fin_db}_body(id);" \
                          f"create unique index {group_name_fin_db}_from_id_uindex on all_messages.{group_name_fin_db}_from(data);" \
                          f"create unique index {group_name_fin_db}_subjects_id_uindex on all_messages.{group_name_fin_db}_subjects(subject);"
                    db_cursor = configuration.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    db_cursor.close()
                except Exception:
                    pass

        except Exception:
            print("Exception #: 4")
            exit()

        count = count + 1
        # Set initial path for where to unzip MBOX files
        where2unzip = configuration.path2unzip + filename

        try:
            f = open(where2unzip)
            f.close()
            print("MBOX file was already unzipped and found at: " + where2unzip)
        except IOError:
            print("MBOX Unzipped file not there yet!")
            # Unzip MBOX.GZ and Place to TMP
            print("Starting to Unzip: " + str(count) + " - " + str(f))
            with gzip.open(f, 'rb') as f_in:
                with open(where2unzip, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print("Unzipped to: " + where2unzip)
            print("Starting to Process MBOX")

        mbox = mailbox.mbox(where2unzip)

        print("**************************************")


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

        def removeNonAscii(s): return "".join(i for i in s if ord(i)<126 and ord(i)>31)

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


        # Process every single mesage recovered from the MBOX file
        for message in mbox:
            processing_message_counter = processing_message_counter + 1

            # start only processing once you get to first unprocessed message
            if processing_message_counter > current_position_in_db:

                try:
                    message = email.message_from_string(str(message), policy=policy.default)
                except Exception:
                    pass

                all_count = int(mbox._next_key)

                if processing_message_counter % 1000 == 0:
                    percentage = round(100 * float(processing_message_counter) / float(all_count), 2)

                    # Show how many messsages we're processing per minute
                    # sql_count = "SELECT COUNT(*) FROM all_messages.headers WHERE processed >= (now() - INTERVAL \'1 MINUTE\')"
                    # sql_count = "SELECT COUNT(*) FROM all_messages.headers"
                    try:
                        sql = f"SELECT COUNT(*) FROM all_messages.{group_name_fin_db}_headers WHERE processed >= (now() - INTERVAL '1 MINUTE')"
                        db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        messages_per_minute1 = db_cursor.fetchone()[0]
                        db_cursor.close()
                    except Exception:
                        print("Exception #: 5")
                        exit()

                    # print(message_body)
                    try:
                        sql = f"INSERT INTO all_messages.__all_updates(groupname,perminute) VALUES ((%s), (%s))"
                        db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql, (filename, messages_per_minute1))
                        configuration.db_connection.commit()
                        db_cursor.close()
                    except Exception as err:
                        print(err.pgerror)
                        print("Exception #: 6")
                        exit()

                    # Delete all execept last 100 most recent update messages
                    try:
                        sql = f"DELETE FROM all_messages.__all_updates WHERE id <= (SELECT id FROM (SELECT id FROM all_messages.__all_updates ORDER BY id DESC LIMIT 1 OFFSET 100) foo);"
                        db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        db_cursor.close()
                    except Exception:
                        print("Exception #: 7")
                        exit()

                    try:
                        sql = f"select SUM(perminute) from all_messages.__all_updates where id in (SELECT MAX(id) as t FROM all_messages.__all_updates WHERE tstamp >= (now() - INTERVAL '1 MINUTE') group by groupname);"
                        db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        messages_per_minute1 = db_cursor.fetchone()[0]
                        db_cursor.close()
                        if not messages_per_minute1:
                            messages_per_minute1 = 0
                        print(filename.replace(".mbox", "") + ": " + str(processing_message_counter) + " of " + str(
                            all_count) + " (" + str(percentage) + "%) | " + str(
                            groupnum(messages_per_minute1)) + " msgs/min (" + str(
                            groupnum(messages_per_minute1 * 60)) + " hr, " + str(
                            groupnum(messages_per_minute1 * 60 * 24)) + " day, " + str(
                            groupnum(messages_per_minute1 * 60 * 24 * 365)) + " year)")
                    except Exception:
                        print("Exception #: 8")
                        exit()

                # RESET ALL VARS
                parsed_encoding = None
                parsed_content_type = None
                parsed_message_id = None
                parsed_date = None
                parsed_subject = None
                parsed_subject_original = None
                parsed_ref = None
                parsed_body_text = None
                parsed_body_text_original = None
                parsed_from = None
                parsed_from_original = None
                has_ref = 0


                #############################################
                # USENET HEADER PARSING
                #############################################
                # GET HEADERS IN ORIGINAL RAW FORMAT (NOT UTF-8)
                # PARSE THE IMPORTANT PARTS FROM LIST OF HEADERS

                for p in message._headers:
                    name = str(p[0]).lower()

                    # Parse Date
                    if name == 'date':
                        parsed_date = p[1]

                    # Parse Content Type
                    if name == 'content-type':
                        parsed_content_type = p[1]

                    # Parse content-transfer-encoding
                    if name == 'content-transfer-encoding':
                        parsed_content_type = p[1]

                    # Parse References
                    if name == 'references':
                        parsed_ref = p[1]

                    # Parse Subject
                    if name == 'subject':
                        parsed_subject = p[1]
                        parsed_subject_original = p[1]

                    # Parse message-id
                    if name == 'message-id':
                        parsed_message_id = p[1]

                    # Parse From
                    if name == 'from':
                        parsed_from = p[1]
                        parsed_from_original = p[1]

                    # Parse Charset Encoding
                    if name == 'content-type':
                        try:
                            parsed_encoding = message.get_content_charset()
                        except Exception:
                            if "charset=" in name:
                                try:
                                    parsed_encoding = str(
                                        re.findall(r'"([^"]*)"', str(p[1].rstrip(os.linesep).replace("\n", "")))[0])
                                except Exception:
                                    dammit = UnicodeDammit(p[1].rstrip(os.linesep).replace("\n", ""))
                                    parsed_encoding = dammit.original_encoding
                            else:
                                dammit = UnicodeDammit(p[1].rstrip(os.linesep).replace("\n", ""))
                                parsed_encoding = dammit.original_encoding

                    #############################################
                    # DATA CLEAN UP - message_references
                    #############################################

                # GET BODY OF THE MESSAGE
                try:
                    parsed_body_text_original = message.get_payload(decode=False)
                    if message.is_multipart():
                        for part in message.walk():
                            if part.is_multipart():
                                for subpart in part.walk():
                                    if subpart.get_content_type() == 'text/plain':
                                        parsed_body_text = subpart.get_content()
                            elif part.get_content_type() == 'text/plain':
                                parsed_body_text = part.get_content()
                    elif message.get_content_type() == 'text/plain':
                        try:
                            parsed_body_text = message.get_content()
                            try:
                                parsed_body_text.encode('utf-8', 'surrogatepass')
                            except Exception:
                                parsed_body_text = message.get_payload(decode=False)
                        except Exception:
                            parsed_body_text = message.get_payload(decode=False)
                    # parsed_body_text = parsed_message._mail['body']
                except Exception:
                    # dammit = UnicodeDammit(str(parsed_body_text).encode('utf-8', 'surrogatepass'))
                    # parsed_body_text = str(parsed_body_text).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                    try:
                        if message.is_multipart():
                            for part in message.walk():
                                if part.is_multipart():
                                    for subpart in part.walk():
                                        if subpart.get_content_type() == 'text/plain':
                                            parsed_body_text = subpart.get_payload(decode=True)
                                elif part.get_content_type() == 'text/plain':
                                    parsed_body_text = str(part.get_payload(decode=True))
                        elif message.get_content_type() == 'text/plain':
                            parsed_body_text1 = message.get_payload(decode=True)
                            parsed_body_text = message.get_payload(decode=False)
                            parsed_body_text_original = message.get_payload(decode=False)
                            # parsed_body_text = str(message.get_payload(decode=True)).encode('utf-8', 'surrogatepass')
                            dammit = UnicodeDammit(parsed_body_text1)
                            parsed_encoding = dammit.original_encoding
                            # body_text = parsed_message._mail['body']
                    except Exception:
                        parsed_body_text = ""
                        pass

                # DATA CLEAN UP - MESSAGE BODY
                # try:
                #     if parsed_encoding:
                #         parsed_body_text = parsed_body_text.encode('utf-8', 'surrogatepass').decode(parsed_encoding)
                #     else:
                #         dammit_body = UnicodeDammit(str(parsed_body_text).encode('utf-8', 'surrogatepass'))
                #         parsed_body_text = str(parsed_body_text).encode('utf-8', 'surrogatepass').decode(
                #             dammit_body.original_encoding)
                # except Exception:
                #     dammit_body = UnicodeDammit(str(parsed_body_text).encode('utf-8', 'surrogatepass'))
                #     parsed_body_text = str(parsed_body_text).encode('utf-8', 'surrogatepass').decode("ANSI")

                # DATA CLEAN UP - DATE

                try:
                    if '(' in parsed_date:
                        parsed_date = message['date'].split("(")[0].strip()
                    else:
                        parsed_date = message['date'].strip()
                except Exception:
                    pass

                failing_zones_to_check = ['-13', '-14', '-15', '-16', '-17', '-18', '-19', '-20', '-21', '-22', '-23', '-24', '+15', '+16', '+17', '+18', '+19', '+20', '+21', '+22', '+23', '+24']
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
                        parsed_date = message['NNTP-Posting-Date']
                        for failedzone in failing_zones_to_check:
                            if failedzone in parsed_date:
                                parsed_date = parsed_date.split(failedzone)[0]
                                print ('Fixed NNTP: ' + parsed_date + ' | ' + failedzone)
                                break
                        else:
                            parsed_date = dateutil.parser.parse(parsed_date, tzinfos=configuration.timezone_info)
                    except Exception:
                        # new_headers.append(tuple(("odate", value)))
                        continue

                        # DATA CLEAN UP - message_encoding
                if not parsed_encoding:
                    parsed_encoding = "ANSI"
                elif parsed_encoding == "x-user-defined":
                    parsed_encoding = "ANSI"

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
                if parsed_subject:
                    parsed_subject = clean_string(parsed_subject, parsed_encoding)
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

                try:
                    # Check If MSG ID already in db
                    db_cursor = configuration.db_connection.cursor()
                    parsed_message_id = removeNonAscii(parsed_message_id)
                    query = f"select exists (select * from all_messages.{group_name_fin_db}_headers where msg_id=\'" + parsed_message_id + "\');"
                    db_cursor.execute(query)
                    msg_exist = db_cursor.fetchone()[0]
                    db_cursor.close()
                except Exception:
                    print("Exception #: 9")
                    try:
                        # Check If MSG ID already in db
                        db_cursor = configuration.db_connection.cursor()

                        query = f"select exists (select * from all_messages.{group_name_fin_db}_headers where msg_id=\'{parsed_message_id}\')"
                        db_cursor.execute(query)
                        msg_exist = db_cursor.fetchone()[0]
                        db_cursor.close()
                    except Exception:
                        print("Passing: " + parsed_message_id)
                        print("Exception #: 10")
                        msg_exist = False
                    pass


            # Continue only if MSG not in the headers db
                if not msg_exist:
                    try:
                        try:
                            # Add a unique subject line
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) returning id"
                            db_cursor = configuration.db_connection.cursor()
                            db_cursor.execute(sql, (parsed_subject, parsed_subject))
                            configuration.db_connection.commit()
                            inserted_subject_id = db_cursor.fetchone()[0]
                            db_cursor.close()
                        except Exception:
                            print("Exception #: 11")
                            #exit()
                            if inserted_subject_id is None:
                                try:
                                    parsed_subject = parsed_subject.encode("ascii", "ignore").decode()
                                    parsed_subject = re.sub(r'[^\x00-\x7f]', r'', parsed_subject)
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) returning id"
                                    db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (parsed_subject, parsed_subject))
                                    configuration.db_connection.commit()
                                    inserted_subject_id = db_cursor.fetchone()[0]
                                    db_cursor.close()
                                except Exception:
                                    print("Exception #: 12")
                                    #exit()
                                    try:
                                        parsed_subject = re.sub(r'[^\x00-\x7f]', r'', parsed_subject_original)
                                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) returning id"
                                        db_cursor = configuration.db_connection.cursor()
                                        db_cursor.execute(sql, (parsed_subject, parsed_subject))
                                        configuration.db_connection.commit()
                                        inserted_subject_id = db_cursor.fetchone()[0]
                                        db_cursor.close()
                                    except Exception:
                                        print("Exception #: 13")
                                        #exit()
                                        pass


                        try:
                            # Add a unique from line
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES ((%s)) ON CONFLICT(data) DO UPDATE SET data=(%s) returning id"
                            db_cursor = configuration.db_connection.cursor()
                            db_cursor.execute(sql, (parsed_from, parsed_from))
                            configuration.db_connection.commit()
                            inserted_from_id = db_cursor.fetchone()[0]
                            db_cursor.close()
                        except Exception:
                            print("Exception #: 14")
                            #exit()
                            if inserted_from_id is None:
                                try:
                                    parsed_from = parsed_from.encode("ascii", "ignore").decode()
                                    parsed_from = re.sub(r'[^\x00-\x7f]', r'', parsed_from)
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES ((%s)) ON CONFLICT(data) DO UPDATE SET data=(%s) returning id"
                                    db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (parsed_from, parsed_from))
                                    configuration.db_connection.commit()
                                    inserted_from_id = db_cursor.fetchone()[0]
                                    db_cursor.close()
                                except Exception:
                                    print("Exception #: 15")
                                    #exit()
                                    parsed_from = re.sub(r'[^\x00-\x7f]', r'', parsed_from_original)
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES ((%s)) ON CONFLICT(data) DO UPDATE SET data=(%s) returning id"
                                    db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (parsed_from, parsed_from))
                                    configuration.db_connection.commit()
                                    inserted_from_id = db_cursor.fetchone()[0]
                                    db_cursor.close()
                        # Add a header info - pass in the subject line id from the previous statement

                        if parsed_ref:
                            has_ref = 1
                        else:
                            has_ref = 0

                        try:
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_headers(dateparsed, subj_id, ref, msg_id, msg_from, enc, contype) VALUES ((%s), (%s), (%s), (%s), (%s), (%s), (%s)) RETURNING id"
                            db_cursor = configuration.db_connection.cursor()
                            db_cursor.execute(sql, (
                                parsed_date, inserted_subject_id, has_ref, parsed_message_id, inserted_from_id, parsed_encoding,
                                parsed_content_type))
                            configuration.db_connection.commit()
                            inserted_header_id = db_cursor.fetchone()[0]
                            db_cursor.close()
                        except Exception:
                            print("Exception #: 16")
                            #exit()
                            print('Duplicate MSG ID: ' + parsed_message_id)

                            continue

                        if parsed_ref:
                            split_refs = parsed_ref.split(' ')
                            for split in split_refs:
                                try:
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_refs(id, ref_msg) VALUES ((%s), (%s));"
                                    db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (inserted_header_id, split.strip()))
                                    configuration.db_connection.commit()
                                    db_cursor.close()
                                except Exception:
                                    print("Exception #: 17")
                                    #exit()
                                    pass
                        try:
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                            db_cursor = configuration.db_connection.cursor()
                            db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                            configuration.db_connection.commit()
                            db_cursor.close()
                        except Exception:
                            print("Exception #: 18")
                            try:
                                parsed_body_text = parsed_body_text.encode("ascii", "ignore").decode()
                                parsed_body_text = re.sub(r'[^\x00-\x7f]', r'', parsed_body_text)
                                sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                                db_cursor = configuration.db_connection.cursor()
                                db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                                configuration.db_connection.commit()
                                db_cursor.close()
                            except Exception:
                                print("Exception #: 19")
                                #parsed_body_text = parsed_body_text_original.encode('utf-8', 'surrogateescape').decode('ANSI')
                                try:
                                    parsed_body_text = re.sub(r'[^\x00-\x7f]', r'', parsed_body_text)
                                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                                    db_cursor = configuration.db_connection.cursor()
                                    db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                                    configuration.db_connection.commit()
                                    db_cursor.close()
                                except Exception:
                                    print("Exception #: 19")
                                    continue


                    except Exception as err:
                        print("Exception #: 20")
                        print("------------------------")
                        print("-*-" + str(sql) + "-*-")
                        print("-*-" + str(parsed_message_id) + "-*-")
                        print("-*-" + str(parsed_date) + "-*-")
                        print("-*-" + str(parsed_from) + "-*-")
                        print("-*-" + str(parsed_subject) + "-*-")
                        print("-*-" + str(parsed_ref) + "-*-")
                        print("-*-" + str(parsed_encoding) + "-*-")
                        print("-*-" + str(parsed_content_type) + "-*-")
                        print("-*-" + str(parsed_body_text) + "-*-")
                        print("------------------------")
                        print_psycopg2_exception(err)
                        print(str(processing_message_counter) + " - " + str(err))
                        print("------------------------")


                all_count = int(mbox._next_key)
                # group_name_fin = file_name
                # update DB - marked file as not being processed anymore

                if processing_message_counter == all_count:
                    try:
                        sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}',{processing_message_counter},{all_count},0,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET current={processing_message_counter}, total={all_count}, processing=0"
                        db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        configuration.db_connection.commit()
                        db_cursor.close()
                    except Exception as err:
                        print("Exception #: 21")
                else:
                    try:
                        sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}',{processing_message_counter},{all_count},1,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET current={processing_message_counter}, total={all_count}, processing=1"
                        db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        configuration.db_connection.commit()
                        db_cursor.close()
                    except Exception as err:
                        print("Exception #: 22")

        # remove temp file
if os.path.exists(where2unzip):
    mbox.unlock()
    mbox.close()

    try:
        os.remove(where2unzip)
        print("** TEMP file removed: " + where2unzip)
    except Exception:
        print(Exception)

    try:
        f.close()
        shutil.move(f, configuration.processed_path + '/' + filename + '.gz')
        print('Moving File to ' + configuration.processed_path + filename + '.gz')
    except Exception:
        print(Exception)


else:
    print("The file does not exist: " + where2unzip)
