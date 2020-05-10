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
from datetime import date
from email import policy

import cchardet
import dateutil.parser
# START - CONFIGURATION
from bs4 import UnicodeDammit

import configuration_json

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

path = configuration_json.path.replace("\\", "/") + "/"

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
        sql = f"SELECT * FROM all_messages.all_files WHERE file_name = '{filename}' LIMIT 1"
        db_cursor = configuration_json.db_connection.cursor()
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

    if (current_position_in_db > 0) and (current_position_in_db == last_message_count):
        # Move a file from the directory d1 to d2
        try:
            shutil.move(f, configuration_json.processed_path + filename + '.gz')
            print("Moving File: " + filename + ".gz")
        except Exception:
            pass

    if (file_name == "") or (
            current_position_in_db == 0 and last_message_count == 0 and is_file_being_processed == 0) or (
            current_position_in_db < last_message_count and is_file_being_processed == 0):
        try:
            filename_extract = filename.replace(".mbox", "")
            group_name_fin = filename_extract.replace("." + filename_extract.split(".")[-1], "")
            group_name_fin_db = group_name_fin.replace(".", "_").replace("-", "_").replace("+", "")

            sql = f"INSERT INTO all_messages.all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}', 0, 0 ,1,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET processing=1"
            # sql = "INSERT INTO all_messages.all_files(file_name, current, total, processing, newsgroup_name) VALUES ('sci.homebrew.20140221.mbox', 0, 0 ,1,'sci.homebrew') ON CONFLICT (file_name) DO UPDATE SET processing=1"
            db_cursor = configuration_json.db_connection.cursor()
            db_cursor.execute(sql)
            configuration_json.db_connection.commit()
            db_cursor.close()

            # Create tables for a new group
            db_cursor = configuration_json.db_connection.cursor()
            db_cursor.execute(
                f"select exists(select * from information_schema.tables where table_name='{group_name_fin_db}_headers')")
            exist = db_cursor.fetchone()[0]
            db_cursor.close()

            if not exist:
                try:
                    sql = f"create table all_messages.{group_name_fin_db}_headers(id bigserial not null constraint {group_name_fin_db}_headers_pk primary key, dateparsed timestamp, subj_id bigint, ref smallint, msg_id text, msg_from text, enc text, contype text, processed timestamp default CURRENT_TIMESTAMP);alter table all_messages.{group_name_fin_db}_headers owner to postgres;"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration_json.db_connection.commit()
                    db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_refs(id bigint, ref_msg text default null);alter table all_messages.{group_name_fin_db}_refs owner to postgres;"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration_json.db_connection.commit()
                    db_cursor.close()

                    sql = f"create table all_messages.{group_name_fin_db}_body(id bigint primary key, data text default null);alter table all_messages.{group_name_fin_db}_body owner to postgres;"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration_json.db_connection.commit()
                    db_cursor.close()

                    sql = f"create unique index {group_name_fin_db}_headers_id_uindex on all_messages.{group_name_fin_db}_headers(id);"

                    #     f"create unique index {group_name_fin_db}_refs_id_uindex on all_messages.{group_name_fin_db}_refs(id);" \
                    #     f"create unique index {group_name_fin_db}_body_id_uindex on all_messages.{group_name_fin_db}_body(id);"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration_json.db_connection.commit()
                    db_cursor.close()
                except Exception:
                    pass

        except Exception:
            print('')

        count = count + 1
        # Set initial path for where to unzip MBOX files
        where2unzip = r'C:/tmp/' + filename

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
                    sql = f"SELECT COUNT(*) FROM all_messages.{group_name_fin_db}_headers WHERE processed >= (now() - INTERVAL '1 MINUTE')"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    messages_per_minute1 = db_cursor.fetchone()[0]
                    db_cursor.close()
                    # print('100')

                    # print(message_body)
                    try:
                        sql = f"INSERT INTO all_messages.all_updates(groupname,perminute) VALUES ((%s), (%s))"
                        db_cursor = configuration_json.db_connection.cursor()
                        db_cursor.execute(sql, (filename, messages_per_minute1))
                        configuration_json.db_connection.commit()
                        # sql_message_id = db_cursor.fetchone()[0]
                        db_cursor.close()
                    except Exception as err:
                        print(err.pgerror)

                    sql = f"select SUM(perminute) from all_messages.all_updates where id in (SELECT MAX(id) as t FROM all_messages.all_updates WHERE tstamp >= (now() - INTERVAL '1 MINUTE') group by groupname);"
                    db_cursor = configuration_json.db_connection.cursor()
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

                # RESET ALL VARS
                sql_body_id = None
                sql_message_id = None
                sql_subject_id = None
                reference_id = None
                sql_newsgroup_id = None
                date_time = None
                message_id = None
                subject_text = None
                message_from_email = None
                message_from_name = None
                reply_to_email = None
                reply_to_name = None
                headers_in_json = None
                message_body = None

                parsed_encoding = None
                parsed_content_type = None
                parsed_message_id = None
                parsed_date = None
                parsed_subject = None
                parsed_ref = None
                parsed_body_text = None
                parsed_from = None
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
                        parsed_date = p[1].rstrip(os.linesep).replace("\n", "")

                    # Parse Content Type 7/8bit - goes to JSON
                    if name == 'content-type':
                        parsed_content_type = str(p[1].rstrip(os.linesep).replace("\n", ""))

                    # Parse content-transfer-encoding
                    if name == 'content-transfer-encoding':
                        parsed_content_type = str(p[1].rstrip(os.linesep).replace("\n", ""))

                    # Parse References
                    if name == 'references':
                        parsed_ref = p[1].rstrip(os.linesep).replace("\n", "")

                    # Parse Subject
                    if name == 'subject':
                        parsed_subject = p[1].rstrip(os.linesep).replace("\n", "")

                    # Parse message-id
                    if name == 'message-id':
                        parsed_message_id = p[1].rstrip(os.linesep).replace("\n", "")

                    # Parse From
                    if name == 'from':
                        parsed_from = p[1].rstrip(os.linesep).replace("\n", "")

                    # Parse Charset Encoding  - goes to JSON
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
                try:

                    # GET BODY OF THE MESSAGE
                    try:
                        if message.is_multipart():
                            for part in message.walk():
                                if part.is_multipart():
                                    for subpart in part.walk():
                                        if subpart.get_content_type() == 'text/plain':
                                            parsed_body_text = subpart.get_content()
                                elif part.get_content_type() == 'text/plain':
                                    parsed_body_text = part.get_content()
                        elif message.get_content_type() == 'text/plain':
                            parsed_body_text = message.get_content()
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
                                #parsed_body_text = str(message.get_payload(decode=True)).encode('utf-8', 'surrogatepass')
                                dammit = UnicodeDammit(parsed_body_text1)
                                parsed_encoding = dammit.original_encoding
                                # body_text = parsed_message._mail['body']
                        except Exception:
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
                        parsed_date = dateutil.parser.parse(message['date'], tzinfos=configuration_json.timezone_info)
                    except Exception:
                        try:
                            # Try to parse/convert NNTP-Posting-Date
                            value = dateutil.parser.parse(message['NNTP-Posting-Date'],
                                                          tzinfos=configuration_json.timezone_info)
                            parsed_date = value
                            # new_headers.append(tuple((name, str(value))))
                        except Exception:
                            # new_headers.append(tuple(("odate", value)))
                            pass

                    # DATA CLEAN UP - message_encoding
                    if parsed_encoding:
                        try:
                            parsed_encoding = parsed_encoding.encode('utf-8', 'surrogatepass').decode(parsed_encoding)
                        except Exception:
                            dammit = UnicodeDammit(parsed_encoding)
                            parsed_encoding = str(p[1].rstrip(os.linesep).replace("\n", "")).encode('utf-8', 'surrogatepass').decode(
                                dammit.original_encoding)
                    else:
                        parsed_encoding = "ANSI"

                    if parsed_ref:
                        try:
                            parsed_ref = parsed_ref.encode('utf-8', 'surrogatepass').decode(parsed_encoding)
                        except Exception:
                            dammit = UnicodeDammit(parsed_ref.encode('utf-8', 'surrogatepass'))
                            parsed_ref = parsed_ref.encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)


                    # DATA CLEAN UP - message_id
                    if parsed_message_id:
                        parsed_message_id = parsed_message_id.encode('utf-8', 'surrogatepass').decode(parsed_encoding)
                    else:
                        parsed_message_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

                    # DATA CLEAN UP - message_subject
                    try:
                        #parsed_subject = parsed_subject.encode(parsed_encoding, 'surrogatepass').decode('utf-8')
                        parsed_subject = quopri.decodestring(parsed_subject).decode(parsed_encoding)
                        if '?q?' in parsed_subject:
                            parsed_subject = find_between(parsed_subject, 'q?', '?').replace("_", " ")
                        elif '?Q?' in parsed_subject:
                            parsed_subject = find_between(parsed_subject, 'Q?', '?').replace("_", " ")
                            # print(parsed_from)
                    except Exception:
                        if parsed_subject:
                            dammit = UnicodeDammit(parsed_subject)
                            parsed_subject = parsed_subject.encode(dammit.original_encoding, 'surrogatepass').decode('utf-8')
                        else:
                            parsed_subject = ""


                    # DATA CLEAN UP - message_from
                    try:
                        parsed_from = quopri.decodestring(parsed_from).decode(parsed_encoding)
                        if '?q?' in parsed_from:
                            parsed_from = find_between(parsed_from, 'q?', '?').replace("_", " ")
                        if '?Q?' in parsed_subject:
                            parsed_subject = find_between(parsed_subject, 'Q?', '?').replace("_", " ")
                        # print(parsed_from)
                    except Exception:
                        pass
                    #############################################
                    # Add everything that goes into JSONB headers table, into new list
                    #############################################
                    # new_headers.append(tuple(("enc", parsed_encoding)))
                    # new_headers.append(tuple(("tenc", parsed_encoding)))
                    # new_headers.append(tuple(("from", parsed_from)))
                    # Add it in JSON format
                    # headers_in_json = json.dumps(dict(new_headers)) # print(headers_in_json)

                    #############################################
                    # Add compression if needed
                    #############################################
                    # compressed_message_body = zlib.compress(message_body.encode('utf-8'))
                    # decompressed = zlib.decompress(compressed_message_body)
                    # decompressed_message_body = zlib.decompress(compressed_message_body)

                    #############################################
                    # ADD MESSAGE DETAILS INTO POSTGRES
                    #############################################
                    # Add a unique subject line
                    sql = f"INSERT INTO all_messages.all_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) returning id"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql, (parsed_subject, parsed_subject))
                    configuration_json.db_connection.commit()
                    inserted_subject_id = db_cursor.fetchone()[0]
                    db_cursor.close()

                    # Add a header info - pass in the subject line id from the previous statement

                    if parsed_ref:
                        has_ref = 1
                    else:
                        has_ref = 0

                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_headers(dateparsed, subj_id, ref, msg_id, msg_from, enc, contype) VALUES ((%s), (%s), (%s), (%s), (%s), (%s), (%s)) RETURNING id"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql, (
                        parsed_date, inserted_subject_id, has_ref, parsed_message_id, parsed_from, parsed_encoding,
                        parsed_content_type))
                    configuration_json.db_connection.commit()
                    inserted_header_id = db_cursor.fetchone()[0]
                    db_cursor.close()

                    if parsed_ref:
                        split_refs = parsed_ref.split(' ')
                        for split in split_refs:
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_refs(id, ref_msg) VALUES ((%s), (%s));"
                            db_cursor = configuration_json.db_connection.cursor()
                            db_cursor.execute(sql, (inserted_header_id, split.strip()))
                            configuration_json.db_connection.commit()
                            db_cursor.close()

                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                    configuration_json.db_connection.commit()
                    # sql_message_id = db_cursor.fetchone()[0]
                    db_cursor.close()

                    all_count = int(mbox._next_key)
                    # group_name_fin = file_name
                    sql = f"INSERT INTO all_messages.all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}',{processing_message_counter},{all_count},1,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET current={processing_message_counter}, total={all_count}, processing=1"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration_json.db_connection.commit()
                    # sql_message_id = db_cursor.fetchone()[0]
                    db_cursor.close()

                    # update DB - marked file as not being processed anymore
                    if processing_message_counter == all_count:
                        sql = f"INSERT INTO all_messages.all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}',{processing_message_counter},{all_count},0,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET current={processing_message_counter}, total={all_count}, processing=0"
                        db_cursor = configuration_json.db_connection.cursor()
                        db_cursor.execute(sql)
                        configuration_json.db_connection.commit()
                        # sql_message_id = db_cursor.fetchone()[0]
                        db_cursor.close()

                except Exception as err:
                    print_psycopg2_exception(err)
                    print(processing_message_counter + "- " + headers_in_json)
                    print(processing_message_counter + "- " + message_body)
                    print("-------------------")

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
        shutil.move(f, configuration_json.processed_path + '\\' + filename + '.gz')
        print('Moving File to ' + configuration_json.processed_path + filename + '.gz')
    except Exception:
        print(Exception)


else:
    print("The file does not exist: " + where2unzip)
