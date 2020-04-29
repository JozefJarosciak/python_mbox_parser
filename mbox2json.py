import encodings
import ntpath
import os
import json
import urllib
from datetime import date
from time import strftime
import chardet
import cchardet
import dateutil.parser
import glob
import gzip
import random
import re
import shutil
import mailbox as mailbox
import string
import unidecode
import zlib
import psycopg2

# Wait random time 15-120 seconds before starting
# sleep_time = random.randint(1, 120)
# print("Waiting " + str(sleep_time) + " seconds before start!")
# time.sleep(sleep_time)

# START - CONFIGURATION
from bs4 import UnicodeDammit
from dateutil.tz import gettz

import configuration_json

where2unzip = ""
print("Connecting PostgreSQL DB")

today = date.today()
print("Starting at:", today)

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
processing_message_counter = 0
# path = r"C:\tmp"
path = r"E:\GiganewsArchives\giganews\downloads\0.processing\to do"
path = path.replace("\\", "/") + "/"

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

    if (file_name == "") or (current_position_in_db == 0 and last_message_count == 0 and is_file_being_processed == 0) or (current_position_in_db < last_message_count and is_file_being_processed == 0):
        try:
            filename_extract = filename.replace(".mbox", "")
            group_name_fin = filename_extract.replace("." + filename_extract.split(".")[-1], "")
            group_name_fin_db = group_name_fin.replace(".","_").replace("-", "_")

            sql = f"INSERT INTO all_messages.all_files(file_name, current, total, processing, newsgroup_name) VALUES ('{filename}', 0, 0 ,1,'{group_name_fin}') ON CONFLICT (file_name) DO UPDATE SET processing=1"
            # sql = "INSERT INTO all_messages.all_files(file_name, current, total, processing, newsgroup_name) VALUES ('sci.homebrew.20140221.mbox', 0, 0 ,1,'sci.homebrew') ON CONFLICT (file_name) DO UPDATE SET processing=1"
            db_cursor = configuration_json.db_connection.cursor()
            db_cursor.execute(sql)
            configuration_json.db_connection.commit()
            db_cursor.close()

            # Create tables for a new group
            db_cursor = configuration_json.db_connection.cursor()
            db_cursor.execute(f"select exists(select * from information_schema.tables where table_name='{group_name_fin_db}_headers')")
            exist = db_cursor.fetchone()[0]
            db_cursor.close()

            if not exist:
                try:
                    sql = f"create table all_messages.{group_name_fin_db}_headers(id bigserial not null constraint {group_name_fin_db}_headers_pk primary key, data jsonb, processed timestamp default CURRENT_TIMESTAMP);" \
                          f"alter table all_messages.{group_name_fin_db}_headers owner to postgres; create unique index {group_name_fin_db}_headers_id_uindex on all_messages.{group_name_fin_db}_headers (id);" \
                          f"create table all_messages.{group_name_fin_db}_body(id integer,data text);alter table all_messages.{group_name_fin_db}_body owner to postgres;"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    #configuration_json.db_connection.commit()
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
        # if count == 1: messages_per_minute()  # start minute long updates

        for message in mbox:

            all_count = int(mbox._next_key)
            processing_message_counter = processing_message_counter + 1

            if processing_message_counter > current_position_in_db:

                if processing_message_counter % 1000 == 0:
                    percentage = round(100 * float(processing_message_counter) / float(all_count), 2)

                    # Show how many messsages we're processing per minute
                    #sql_count = "SELECT COUNT(*) FROM all_messages.headers WHERE processed >= (now() - INTERVAL \'1 MINUTE\')"
                    # sql_count = "SELECT COUNT(*) FROM all_messages.headers"
                    sql = f"SELECT COUNT(*) FROM all_messages.{group_name_fin_db}_headers WHERE processed >= (now() - INTERVAL '1 MINUTE')"
                    db_cursor = configuration_json.db_connection.cursor()
                    db_cursor.execute(sql)
                    messages_per_minute1 = db_cursor.fetchone()[0]
                    db_cursor.close()
                    #print('100')
                    print(filename.replace(".mbox", "") + ": " + str(processing_message_counter) + " of " + str(all_count) + " (" + str(percentage) + "%) | " + str(messages_per_minute1) + " msgs/min (" + str(messages_per_minute1 * 60) + " hr, " + str(messages_per_minute1 * 60 * 24) + " day)")

                # RESET ALL VARS
                sql_body_id = None
                sql_message_id = None
                sql_subject_id = None
                reference_id = None
                sql_newsgroup_id = None
                date_time = None
                message_id = None
                subject_text = None
                body_text = None
                message_from_email = None
                message_from_name = None
                reply_to_email = None
                reply_to_name = None
                headers_in_json = None
                message_body = None
                # if message['Content-Transfer-Encoding']:
                #     ContentTransferEncoding = message['Content-Transfer-Encoding']
                #     # print(ContentTransferEncoding)
                # if message['Content-Type']:
                #     ContentType = re.findall(r'"([^"]*)"', message['Content-Type'])
                #     # print(ContentType)

                try:

                    # PARSE MOST IMPORTANT PARTS

                    headers = message._headers
                    new_headers = []
                    for p in headers:
                        name = p[0].rstrip(os.linesep).replace("\n", "").lower()
                        value = p[1].rstrip(os.linesep).replace("\n", "")

                        # Parse Date
                        if name == 'date':
                            new_headers.append(tuple(("orig-date", value)))
                            try:
                                value = dateutil.parser.parse(message['date'], tzinfos = {
                                    'PST': dateutil.tz.gettz('US/Pacific'),
                                    'PDT': dateutil.tz.gettz('US/Pacific'),
                                    'PT': dateutil.tz.gettz('US/Pacific'),
                                    'MST': dateutil.tz.gettz('US/Mountain'),
                                    'MDT': dateutil.tz.gettz('US/Mountain'),
                                    'MT': dateutil.tz.gettz('US/Mountain'),
                                    'CST': dateutil.tz.gettz('US/Central'),
                                    'CDT': dateutil.tz.gettz('US/Central'),
                                    'CT': dateutil.tz.gettz('US/Central'),
                                    'EST': dateutil.tz.gettz('US/Eastern'),
                                    'EDT': dateutil.tz.gettz('US/Eastern'),
                                    'ET': dateutil.tz.gettz('US/Eastern')})
                                new_headers.append(tuple((name, str(value))))
                            except Exception:
                                try:
                                    # Try to parse/convert NNTP-Posting-Date
                                    value = dateutil.parser.parse(message['NNTP-Posting-Date'], tzinfos = {
                                        'PST': dateutil.tz.gettz('US/Pacific'),
                                        'PDT': dateutil.tz.gettz('US/Pacific'),
                                        'PT': dateutil.tz.gettz('US/Pacific'),
                                        'MST': dateutil.tz.gettz('US/Mountain'),
                                        'MDT': dateutil.tz.gettz('US/Mountain'),
                                        'MT': dateutil.tz.gettz('US/Mountain'),
                                        'CST': dateutil.tz.gettz('US/Central'),
                                        'CDT': dateutil.tz.gettz('US/Central'),
                                        'CT': dateutil.tz.gettz('US/Central'),
                                        'EST': dateutil.tz.gettz('US/Eastern'),
                                        'EDT': dateutil.tz.gettz('US/Eastern'),
                                        'ET': dateutil.tz.gettz('US/Eastern')})
                                    new_headers.append(tuple((name, str(value))))
                                except Exception:
                                    new_headers.append(tuple((name, "")))
                                pass

                        # Parse reply-to
                        if name == 'content-type':
                            dammit = UnicodeDammit(str(value).encode('utf-8', 'surrogatepass'))
                            try:
                                message_content = str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                            except Exception:
                                message_content = str(value).encode('utf-8', 'surrogatepass').decode("ANSI")

                            new_headers.append(tuple((name, message_content)))


                        # Parse reply-to
                        if name == 'content-transfer-encoding':
                            dammit = UnicodeDammit(str(value).encode('utf-8', 'surrogatepass'))
                            try:
                                message_encoding = str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                            except Exception:
                                message_encoding = str(value).encode('utf-8', 'surrogatepass').decode("ANSI")

                            new_headers.append(tuple((name, message_encoding)))

                        # Parse References
                        if name == 'references':
                            dammit = UnicodeDammit(str(value).encode('utf-8', 'surrogatepass'))
                            try:
                                message_references = str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                            except Exception:
                                message_references = str(value).encode('utf-8', 'surrogatepass').decode("ANSI")
                            new_headers.append(tuple((name, message_references)))

                        # Parse Subject
                        if name == 'subject':
                            dammit = UnicodeDammit(str(value).encode('utf-8', 'surrogatepass'))
                            try:
                                message_subject = str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                            except Exception:
                                message_subject = str(value).encode('utf-8', 'surrogatepass').decode("ANSI")

                            new_headers.append(tuple((name, message_subject)))
                            #new_headers.append(tuple((name, unidecode.unidecode(re.sub(r"(=\?.*\?=)(?!$)", r"\1 ", value)).replace("'",""))))

                        # Parse message-id
                        if name == 'message-id':
                            if value:
                                dammit = UnicodeDammit(str(value).encode('utf-8', 'surrogatepass'))
                                new_headers.append(tuple((name, str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding))))
                            else:
                                made_up_message_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
                                new_headers.append(tuple(("message-id", made_up_message_id)))

                            # Parse newsgroups
                        if name == 'newsgroups':
                            dammit = UnicodeDammit(value.strip().replace(", ", ",").encode('utf-8', 'surrogatepass'))
                            try:
                                message_newsgroups = str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                            except Exception:
                                message_newsgroups = str(value).encode('utf-8', 'surrogatepass').decode("ANSI")
                            new_headers.append(tuple(("message-id", message_newsgroups)))
                            #new_headers.append(tuple((name, unidecode.unidecode(re.sub(r"(=\?.*\?=)(?!$)", r"\1 ", message_newsgroups)).replace("'", "").replace(" ", "").replace("/", ""))))

                            # Parse From
                        if name == 'from':
                            #encoding = encodings.get(str(value), estimate['encoding'])
                            #estimate = chardet.detect(str(value))
                            dammit = UnicodeDammit(str(value).encode('utf-8', 'surrogatepass'))
                            #print(dammit.unicode_markup)
                            #print(dammit.original_encoding)
                            try:
                                message_from = str(value).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                            except Exception:
                                message_from = str(value).encode('utf-8', 'surrogatepass').decode("ANSI")
                            new_headers.append(tuple(("from", message_from)))
                            #new_headers.append(tuple(("from", unidecode.unidecode(re.sub(r"(=\?.*\?=)(?!$)", r"\1 ", message_from)).replace("'",""))))

                            # try:
                            #     message_from_name = message['from'][0][0]
                            #     message_from_email = message['from'][0][1]
                            #     new_headers.append(tuple(("orig-from", value)))
                            #     new_headers.append(tuple(("name", message_from_name.strip())))
                            #     new_headers.append(tuple(("email", message_from_email.strip())))
                            # except Exception:
                            #     try:
                            #         message_from = str(message['from']).encode('utf-8', 'surrogatepass').decode('utf-8')
                            #         match = re.search(r'[\w\.-]+@[\w\.-]+', message_from)
                            #         message_from_email = match.group(0)
                            #         message_from_name = str(message['from']).replace('"', "").replace('(', "").replace(
                            #             ')', "").replace(message_from_email, "").replace("  ", "").replace("<",
                            #                                                                                "").replace(
                            #             ">", "")
                            #         new_headers.append(tuple(("orig-from", value)))
                            #         new_headers.append(tuple(("name", message_from_name.strip())))
                            #         new_headers.append(tuple(("email", message_from_email.strip())))
                            #     except Exception:
                            #         message_from_name = str(message['from']).encode('utf-8', 'surrogatepass').decode(
                            #             'utf-8').replace("<", "").replace(">", "").replace('(', "").replace(')',
                            #                                                                                 "").replace(
                            #             "  ", "")
                            #         message_from_email = ""
                            #         new_headers.append(tuple(("orig-from", value)))
                            #         new_headers.append(tuple(("name", message_from_name.strip())))
                            #         new_headers.append(tuple(("email", message_from_email.strip())))
                        # new_headers = ('{} : {}'.format(name, value))

                    headers_in_json = json.dumps(dict(new_headers))

                    try:
                        if message.is_multipart():
                            for part in message.walk():
                                if part.is_multipart():
                                    for subpart in part.walk():
                                        if subpart.get_content_type() == 'text/plain':
                                            body_text = subpart.get_payload(decode=True)
                                elif part.get_content_type() == 'text/plain':
                                    body_text = part.get_payload(decode=True)
                        elif message.get_content_type() == 'text/plain':
                            body_text = message.get_payload(decode=True)
                        # body_text = parsed_message._mail['body']
                    except Exception:
                        dammit = UnicodeDammit(str(body_text).encode('utf-8', 'surrogatepass'))
                        body_text = str(body_text).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)

                    #message_body = json.dumps(body_text)
                    dammit = UnicodeDammit(str(body_text).encode('utf-8', 'surrogatepass'))
                    message_body = str(body_text).encode('utf-8', 'surrogatepass').decode(dammit.original_encoding)
                    #compressed_message_body = zlib.compress(message_body.encode('utf-8'))
                    #decompressed = zlib.decompress(compressed_message_body)
                    #decompressed_message_body = zlib.decompress(compressed_message_body)
                    #############################################
                    # Insert Message ID into message_ids table
                    #############################################

                    #print(headers_in_json)
                    try:
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_headers(data) VALUES ((%s)) RETURNING id"
                        db_cursor = configuration_json.db_connection.cursor()
                        db_cursor.execute(sql, (headers_in_json,))
                        configuration_json.db_connection.commit()
                        sql_message_id = db_cursor.fetchone()[0]
                        db_cursor.close()
                    except Exception as err:
                        print(err.pgerror)

                    #print(message_body)
                    try:
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                        db_cursor = configuration_json.db_connection.cursor()
                        db_cursor.execute(sql, (sql_message_id, message_body))
                        configuration_json.db_connection.commit()
                        # sql_message_id = db_cursor.fetchone()[0]
                        db_cursor.close()
                    except Exception as err:
                        print(err.pgerror)

                    all_count = int(mbox._next_key)
                    #group_name_fin = file_name
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

                except Exception as inst:
                    print(processing_message_counter + "- " + headers_in_json)
                    print(processing_message_counter + "- " + message_body)
                    print("-------------------")
                    print(Exception)
                    #pass
            #
            # if "Duplicate entry" not in str(inst.args[1]):
            #     print("Error in message #" + str(processing_message_counter) + ": " + str(inst) + " | " + message_from)
            # else:
            #     pass
            #     # print("Error in message #" + str(processing_message_counter) + ": " + str(inst) + " | " + message_from)

        # if processing_message_counter == 20: exit(0)

        # remove temp file
if os.path.exists(where2unzip):
    #f_in.close()
    #f_out.close()
    mbox.unlock()
    mbox.close()
    try:
        os.remove(where2unzip)
        print("** TEMP file removed: " + where2unzip)
    except Exception:
        pass

    try:
        shutil.move(f, configuration_json.processed_path + '\\' + filename + '.gz')
        print('Moving File to ' +configuration_json.processed_path + '\\' +filename + '.gz')
    except Exception:
        pass
else:
    print("The file does not exist: " + where2unzip)
