import ntpath
import os
import time
from decimal import Decimal

import cchardet
import dateutil.parser
import glob
import gzip
import random
import re
import shutil
import mailbox as mailbox
import string
import threading
import configuration


# Wait random time 15-120 seconds before starting
# sleep_time = random.randint(1, 120)
# print("Waiting " + str(sleep_time) + " seconds before start!")
# time.sleep(sleep_time)

# START - CONFIGURATION
print("Connecting MYSQL DB")
db_cursor = configuration.db_connection.cursor()
db_cursor.execute('SET GLOBAL max_allowed_packet=67108864')
db_cursor.execute('SET GLOBAL max_connections = 500')
#db_cursor.execute('SET GLOBAL max_delayed_threads = 50')
#db_cursor.execute('SET GLOBAL innodb_io_capacity = 5000')


def convert_encoding(data, new_coding='UTF-8'):
    encoding = cchardet.detect(data)['encoding']
    if new_coding.upper() != encoding.upper():
        data = data.decode(encoding, data).encode(new_coding)
    return data



count_minutes = 0
# path = r"C:\tmp"

path = configuration.path.replace("\\", "/") + "/"

# END - CONFIGURATION

# Connect to MySQL




# Start processing MBOX files
print("** START **" + str(path))
print("Processing all files on path: " + str(path))

#foo()

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

    try:
        sql = f"SELECT * FROM all_files WHERE file_name = '{filename}' LIMIT 1"
        # db_cursor = mysql_database.db_connection.cursor()
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

    if (current_position_in_db > 0) and (current_position_in_db == last_message_count):
        # Move a file from the directory d1 to d2
        try:
            shutil.move(f, configuration.processed_path + filename + '.gz')
            print("Moving File: " + filename + ".gz")
        except Exception:
            pass

    if (file_name == "") or (current_position_in_db==0 and last_message_count==0 and is_file_being_processed==0) or (current_position_in_db < last_message_count and is_file_being_processed == 0):

        try:
            sql = f"INSERT INTO all_files(file_name, current, total, processing) VALUE ('{filename}', 0, 0 ,1) ON DUPLICATE KEY UPDATE processing=1"
            # db_cursor = mysql_database.db_connection.cursor()
            db_cursor.execute(sql)
            configuration.db_connection.commit()
            # db_cursor.close()
        except Exception:
            print('')

        count = count + 1
        # Unzip MBOX.GZ and Place to TMP
        print("Starting to Unzip: " + str(count) + " - " + str(f))
        with gzip.open(f, 'rb') as f_in:
            # with open('C:/tmp/'+str(count)+'-'+str(filename), 'wb') as f_out:
            # Set initial path for where to unzip MBOX files
            where2unzip = r'C:/tmp/' + filename
            with open(where2unzip, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Unzipped to: " + where2unzip)
        print("Starting to Process MBOX")
        mbox = mailbox.mbox(where2unzip)

        processing_message_counter = 0
        print("**************************************")
        # if count == 1: messages_per_minute()  # start minute long updates
        for message in mbox:
            all_count = int(mbox._next_key)
            # message = str(message).encode('utf-8', 'ignore').decode('utf-8')
            processing_message_counter = processing_message_counter + 1

            if processing_message_counter > current_position_in_db:
                if processing_message_counter % 100 == 0:
                    percentage = round(100 * float(processing_message_counter) / float(all_count), 2)

                    # Show how many messsages we're processing per minute
                    sql_count = "SELECT COUNT(*) FROM all_messages WHERE processed >= NOW() - INTERVAL 1 MINUTE"
                    db_cursor.execute(sql_count)
                    messages_per_minute1 = db_cursor.fetchone()[0].real
                    print(filename.replace(".mbox", "") + ": " + str(processing_message_counter) + " of " + str(
                        all_count) + " (" + str(percentage) + "%) | " + str(messages_per_minute1) + " msgs/min (" + str(messages_per_minute1*60)+" hr, " + str(messages_per_minute1*60*24) + " day)")
                    # count_minutes = count_minutes + 1
                    # How long it took to processs 100 messages
                    # if count_minutes % 2 != 0:
                    # start_time = time.process_time()
                    # else:
                    # print("100 Messages processed in: " + str((time.process_time() - start_time)*100) + " seconds")

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

                if message['Content-Transfer-Encoding']:
                    ContentTransferEncoding = message['Content-Transfer-Encoding']
                    # print(ContentTransferEncoding)
                if message['Content-Type']:
                    ContentType = re.findall(r'"([^"]*)"', message['Content-Type'])
                    # print(ContentType)

                try:

                    # PARSE MOST IMPORTANT PARTS
                    try:
                        if message['Date']:
                            date_time = dateutil.parser.parse(message['Date'])
                        else:
                            date_time = dateutil.parser.parse('1 Jan 1970 00:00:00 +0000 (UTC)')
                    except Exception:
                        date_time = dateutil.parser.parse('1 Jan 1970 00:00:00 +0000 (UTC)')
                    # if message['Content-Transfer-Encoding']:
                    #   encoding = message['Content-Type']
                    # print(encoding)
                    # ['Path', 'From', 'Newsgroups', 'Subject', 'Date', 'Organization', 'Lines', 'Approved', 'Message-ID', 'References', 'X-Trace', 'X-Complaints-To', 'NNTP-Posting-Date', 'X-Received', 'X-Received', 'X-Path', 'X-Newsgroups', 'X-Priority', 'X-MSMail-Priority', 'X-Newsreader', 'X-MimeOLE', 'X-To', 'Xref']
                    # print(message.keys())
                    # parsed_message = mailparser.parse_from_string(second)

                    #############################################
                    # Insert Message ID into message_ids table
                    #############################################
                    if message['message-id']:
                        message_id = message['message-id']
                    else:
                        message_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
                        # print(message_id)

                    sql = """SELECT id FROM message_ids WHERE messageid = '%s' LIMIT 1""" % message_id
                    # db_cursor = mysql_database.db_connection.cursor()
                    db_cursor.execute(sql)
                    db_cursor.fetchall()
                    number_of_rows1 = db_cursor.rowcount
                    # db_cursor.close()
                    if number_of_rows1 == 0:
                        #############################################
                        # CONTINUE ONLY IF MESSAGE ID WASN'T ALREADY PROCESSED
                        #############################################
                        sql = "INSERT INTO message_ids(messageid) VALUE (%s)"
                        # db_cursor = mysql_database.db_connection.cursor()
                        db_cursor.execute(sql, (message_id,))
                        configuration.db_connection.commit()
                        sql_message_id = db_cursor.lastrowid
                        # db_cursor.close()
                        # get last insert ID
                        # db_cursor.execute(f"SELECT id FROM message_ids WHERE messageid = '{message_id}'  LIMIT 1")
                        # sql_message_id = db_cursor.fetchone()

                        #############################################
                        # Insert Message Subject into message_subject table
                        #############################################
                        try:
                            subject_text = str(message['subject']).encode('utf-8', 'surrogatepass').decode('utf-8')
                        except Exception:
                            subject_text = ""
                        # subject_text2 = eval("b'" + subject_text + "'").decode('utf-8') 'ok SEGA it\'nntp_connection your turn now...'
                        sql = f"SELECT id FROM message_subject_lines WHERE subject = (%s) LIMIT 1"
                        # db_cursor = mysql_database.db_connection.cursor()
                        db_cursor.execute(sql, (subject_text,))
                        db_cursor.fetchall()
                        number_of_rows2 = db_cursor.rowcount
                        # db_cursor.close()
                        if number_of_rows2 == 0:
                            sql = "INSERT INTO message_subject_lines(subject) VALUE (%s)"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql, (subject_text,))
                            configuration.db_connection.commit()
                            sql_subject_id = db_cursor.lastrowid
                            # db_cursor.close()
                        else:
                            sql = f"SELECT id FROM message_subject_lines WHERE subject = (%s) LIMIT 1"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql, (subject_text,))
                            sql_subject_id = db_cursor.fetchone()[0]
                            # db_cursor.close()

                        #############################################
                        # Insert Message Body into message_body table
                        #############################################
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
                            body_text = str(body_text).encode('utf-8', 'surrogatepass').decode('utf-8')
                        try:
                            sql = "INSERT INTO message_body(body) VALUE (%s)"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql, (body_text,))
                            sql_body_id = db_cursor.lastrowid
                            # db_cursor.close()
                        except Exception:
                            body_text = str(body_text).encode('utf-8', 'surrogatepass').decode('utf-8')
                            sql = "INSERT INTO message_body(body) VALUE (%s)"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql, (body_text,))
                            sql_body_id = db_cursor.lastrowid
                            # db_cursor.close()

                        #############################################
                        # Insert From ID into from_contacts table
                        #############################################
                        try:
                            # message_from2 = message['from'].encode('utf-8','surrogatepass').decode('utf-8')
                            message_from_name = message['from'][0][0]
                            message_from_email = message['from'][0][1]
                            print(message_from_name + " | " + message_from_email)
                        except Exception:
                            try:
                                message_from = str(message['from']).encode('utf-8', 'surrogatepass').decode('utf-8')
                                match = re.search(r'[\w\.-]+@[\w\.-]+', message_from)
                                message_from_email = match.group(0)
                                message_from_name = str(message['from']).replace('"', "").replace('(', "").replace(')',"").replace(
                                    message_from_email, "").replace("  ", "").replace("<", "").replace(">", "")
                            except Exception:
                                message_from_name = str(message['from']).encode('utf-8', 'surrogatepass').decode(
                                    'utf-8').replace("<", "").replace(">", "").replace('(', "").replace(')',"").replace("  ", "")
                                message_from_email = ""
                        # message_from_email = message['from'][0].lower()

                        # if message_from_email == 'llknycgu@email.adr':
                        #   print("nntp_connection")

                        #try:
                        #    if message['reply-to'] is not None:
                        #        reply_string = str(message['reply-to'])
                        #        print(reply_string)
                        #    reply_to_name = message['reply-to'][0][0]
                        #    reply_to_email = message['reply-to'][0][1].lower()
                        #except Exception:
                        #    reply_to_name = ""
                        #    reply_to_email = ""

                        sql = f"SELECT id FROM from_contacts WHERE from_email = (%s) LIMIT 1"
                        # db_cursor = mysql_database.db_connection.cursor()
                        db_cursor.execute(sql, (message_from_email.lower(),))
                        db_cursor.fetchall()
                        number_of_rows3 = db_cursor.rowcount
                        # db_cursor.close()
                        if number_of_rows3 == 0:
                            sql = "INSERT INTO from_contacts(from_name, from_email) VALUES ((%s),(%s))"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql,(message_from_name, message_from_email.lower()))
                            configuration.db_connection.commit()
                            sql_from_id = db_cursor.lastrowid
                            # db_cursor.close()
                        else:
                            # get last insert ID
                            sql = f"SELECT id FROM from_contacts WHERE from_email = (%s) LIMIT 1"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql, (message_from_email.lower(),))
                            sql_from_id = db_cursor.fetchone()[0]
                            # db_cursor.close()

                        #############################################
                        # Insert Newsgroup Name into newsgroup_ids table
                        #############################################
                        message_newsgroup_ids = re.sub('\s+', '', message['newsgroups'])
                        newsgroup_names_array = message_newsgroup_ids.split(',')
                        for group_name in newsgroup_names_array:

                            sql = f"SELECT id FROM newsgroup_ids WHERE newsgroupname = '{group_name}' LIMIT 1"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql)
                            db_cursor.fetchall()
                            number_of_rows4 = db_cursor.rowcount
                            # db_cursor.close()
                            if number_of_rows4 == 0:
                                sql = "INSERT INTO newsgroup_ids(newsgroupname) VALUE (%s)"
                                # db_cursor = mysql_database.db_connection.cursor()
                                db_cursor.execute(sql, (group_name,))
                                configuration.db_connection.commit()
                                # db_cursor.close()
                            # get last insert ID
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(
                                f"SELECT id FROM newsgroup_ids WHERE newsgroupname = '{group_name}' LIMIT 1")
                            sql_newsgroup_id = db_cursor.fetchone()
                            # db_cursor.close()
                            #############################################
                            # Update message_newsgroup_ref table with references to which newsgroup this particular message belongs
                            #############################################
                            sql = f"SELECT messageid FROM message_newsgroup_ref WHERE messageid={str(sql_message_id)} AND newsgroup={str(sql_newsgroup_id[0])}"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql)
                            db_cursor.fetchall()
                            number_of_rows5 = db_cursor.rowcount
                            # db_cursor.close()
                            if number_of_rows5 == 0:
                                sql = f"INSERT INTO message_newsgroup_ref (messageid, newsgroup) VALUES ({sql_message_id},{sql_newsgroup_id[0]})"
                                # db_cursor = mysql_database.db_connection.cursor()
                                db_cursor.execute(sql)
                                configuration.db_connection.commit()
                                # db_cursor.close()

                        #############################################
                        # Insert References into references table
                        #############################################
                        has_references = 0
                        if message['references']:
                            ref = str(message['references']).replace('\n', '')
                            # message_references = re.sub('\<.*?\>', ',', ref)
                            # message_references_array = message_references.split(',')
                            regex = re.compile('\<.*?\>')
                            message_references_array = [el.strip('"') for el in regex.findall(ref)]
                            for reference_message_name in message_references_array:
                                sql = f"SELECT id FROM message_ids WHERE messageid='{reference_message_name}' LIMIT 1"
                                # db_cursor = mysql_database.db_connection.cursor()
                                db_cursor.execute(sql)
                                in_db_id = db_cursor.fetchone()
                                # db_cursor.close()
                                has_references = 1
                                if in_db_id:
                                    sql = f"INSERT INTO message_references (messageid, reference) VALUES ({sql_message_id},'{in_db_id[0]}')"
                                    # db_cursor = mysql_database.db_connection.cursor()
                                    db_cursor.execute(sql)
                                    configuration.db_connection.commit()
                                    # db_cursor.close()
                                else:
                                    sql = "INSERT INTO message_ids(messageid) VALUE (%s)"
                                    # db_cursor = mysql_database.db_connection.cursor()
                                    db_cursor.execute(sql, (reference_message_name,))
                                    configuration.db_connection.commit()
                                    reference_id = db_cursor.lastrowid
                                    # db_cursor.close()
                                    sql = f"INSERT INTO message_references (messageid, reference) VALUES ({sql_message_id},'{reference_id}')"
                                    # db_cursor = mysql_database.db_connection.cursor()
                                    db_cursor.execute(sql)
                                    configuration.db_connection.commit()
                                    # db_cursor.close()

                        #############################################
                        # Update all_messages table with references to message id and newsgroup id
                        #############################################
                        sql = f"SELECT has_reference FROM all_messages WHERE messageid={sql_message_id} LIMIT 1"
                        # db_cursor = mysql_database.db_connection.cursor()
                        db_cursor.execute(sql)
                        db_cursor.fetchall()
                        number_of_rows6 = db_cursor.rowcount
                        # db_cursor.close()

                        #                        if message['Date']:
                        #                            date_time = dateutil.parser.parse(message['Date'])
                        #                        else:
                        #                            date_time = dateutil.parser.parse('1 Jan 1970 00:00:00 +0000 (UTC)')

                        if number_of_rows6 == 0:
                            sql = f"INSERT INTO all_messages (messageid, from_contact, date_time, has_reference ,subject, body) VALUES ((%s),(%s),(%s),(%s),(%s),(%s))"
                            # db_cursor = mysql_database.db_connection.cursor()
                            db_cursor.execute(sql, (
                                sql_message_id, sql_from_id, date_time, has_references, sql_subject_id, sql_body_id))
                            configuration.db_connection.commit()
                            # db_cursor.close()

                    # INSERT INTO table (id, name, age) VALUES(1, "A", 19) ON DUPLICATE KEY UPDATE name="A", age=19
                    all_count = int(mbox._next_key)
                    sql = f"INSERT INTO all_files(file_name, current, total, processing) VALUE ('{filename}',{processing_message_counter},{all_count},1) ON DUPLICATE KEY UPDATE current={processing_message_counter}, total={all_count}, processing=1"
                    # db_cursor = mysql_database.db_connection.cursor()
                    db_cursor.execute(sql)
                    configuration.db_connection.commit()
                    sql_subject_id = db_cursor.lastrowid
                    # db_cursor.close()

                    # update DB - marked file as not being processed anymore
                    if processing_message_counter == all_count:
                        sql = f"INSERT INTO all_files(file_name, current, total, processing) VALUE ('{filename}',{processing_message_counter},{all_count},0) ON DUPLICATE KEY UPDATE current={processing_message_counter}, total={all_count}, processing=0"
                        # db_cursor = mysql_database.db_connection.cursor()
                        db_cursor.execute(sql)
                        configuration.db_connection.commit()
                        sql_subject_id = db_cursor.lastrowid
                        # db_cursor.close()

                except Exception as inst:

                    if "Duplicate entry" not in str(inst.args[1]):
                        print("Error in message #" + str(processing_message_counter) + ": " + str(inst) + " | " + message_from)
                    else:
                        pass
                        # print("Error in message #" + str(processing_message_counter) + ": " + str(inst) + " | " + message_from)

        # if processing_message_counter == 20: exit(0)

        # remove temp file
        if os.path.exists(where2unzip):
            f_in.close()
            f_out.close()
            mbox.unlock()
            mbox.close()
            try:
                os.remove(where2unzip)
                print("** TEMP file removed: " + where2unzip)
            except Exception:
                pass

                # Move a file from the directory d1 to d2
            try:
                shutil.move(f, configuration.processed_path + filename + '.gz')
                print("Moving File: " + filename)
            except Exception:
                pass
        else:
            print("The file does not exist: " + where2unzip)
        # exit()
    # ORIGINAL INSERT OF THE JSON CONTENT - RETIRED NOW
    #           sql = "INSERT INTO message(postdate, jsondata) VALUES (%s,%s)"
#           db_cursor.execute(sql, (parsed_message.date, parsed_message.mail_json,))
#           db_connection.commit()
#           print(db_cursor.rowcount, "Record Inserted")


#
#
# CREATE TABLE IF NOT EXISTS `all_files` (
#     `file_name` varchar(255) DEFAULT NULL,
#                                      `current` int(11) DEFAULT NULL,
#                                                                `total` int(11) DEFAULT NULL,
#                                                                                        `processing` tinyint(1) unsigned zerofill NOT NULL,
#                                                                                                                                      UNIQUE KEY `file_name` (`file_name`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `all_messages` (
#     `messageid` int(11) NOT NULL,
#                             `from_contact` int(11) NOT NULL,
#                                                        `date_time` datetime DEFAULT NULL,
#                                                                                     `has_reference` tinyint(1) DEFAULT NULL,
#                                                                                                                        `subject` int(11) NOT NULL,
#                                                                                                                                              `body` int(11) NOT NULL,
#                                                                                                                                                                 `processed` timestamp NOT NULL DEFAULT current_timestamp(),
#                                                                                                                                                                                                        UNIQUE KEY `messageid_newsgroup_from` (`messageid`,`from_contact`),
#                                                                                                                                                                                                                   KEY `messageid` (`messageid`),
#                                                                                                                                                                                                                       KEY `from_contact` (`from_contact`),
#                                                                                                                                                                                                                           KEY `date_time` (`date_time`),
#                                                                                                                                                                                                                               KEY `FK_all_messages_message_subject_lines` (`subject`),
#                                                                                                                                                                                                                                   KEY `FK_all_messages_message_body` (`body`),
#                                                                                                                                                                                                                                       KEY `has_reference` (`has_reference`),
#                                                                                                                                                                                                                                           KEY `all_messages_idx_has_reference` (`has_reference`),
#                                                                                                                                                                                                                                               CONSTRAINT `FK.from_contact2` FOREIGN KEY (`from_contact`) REFERENCES `from_contacts` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
#                                                                                                                                                                                                                                                                                                                                                                        CONSTRAINT `FK.messageid2` FOREIGN KEY (`messageid`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
#                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         CONSTRAINT `FK_all_messages_message_body` FOREIGN KEY (`body`) REFERENCES `message_body` (`id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `from_contacts` (
#     `id` int(11) NOT NULL AUTO_INCREMENT,
#                           `from_name` varchar(255) DEFAULT NULL,
#                                                            `from_email` varchar(255) DEFAULT NULL,
#                                                                                              PRIMARY KEY (`id`),
#                                                                                                      UNIQUE KEY `from_email` (`from_email`),
#                                                                                                                 KEY `from_contacts_idx_id` (`id`)
# ) ENGINE=InnoDB AUTO_INCREMENT=145450 DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `message_body` (
#     `id` int(11) NOT NULL AUTO_INCREMENT,
#                           `body` mediumtext COLLATE utf8mb4_bin DEFAULT NULL,
#                                                                         PRIMARY KEY (`id`),
#                                                                                 KEY `message_body_idx_id` (`id`),
#                                                                                     FULLTEXT KEY `body` (`body`)
# ) ENGINE=InnoDB AUTO_INCREMENT=1211488 DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `message_ids` (
#     `id` int(11) NOT NULL AUTO_INCREMENT,
#                           `messageid` varchar(255) NOT NULL,
#                                                        PRIMARY KEY (`id`),
#                                                                UNIQUE KEY `messageid` (`messageid`),
#                                                                           KEY `messageid-index` (`messageid`),
#                                                                               KEY `message_ids_idx_id` (`id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `message_newsgroup_ref` (
#     `messageid` int(11) NOT NULL,
#                             `newsgroup` int(11) NOT NULL,
#                                                     UNIQUE KEY `messageid_newsgroup_from` (`messageid`,`newsgroup`),
#                                                                KEY `messageid` (`messageid`),
#                                                                    KEY `newsgroup` (`newsgroup`),
#                                                                        KEY `message_newsgroup_re_idx_messageid` (`messageid`),
#                                                                            CONSTRAINT `FK.messageid` FOREIGN KEY (`messageid`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
#                                                                                                                                                                                            CONSTRAINT `FK.newsgroup` FOREIGN KEY (`newsgroup`) REFERENCES `newsgroup_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `message_references` (
#     `messageid` int(11) NOT NULL,
#                             `reference` int(11) NOT NULL,
#                                                     KEY `FK_message_references_message_ids_2` (`reference`),
#                                                         KEY `messageid_reference` (`messageid`,`reference`),
#                                                             KEY `message_references_idx_reference` (`reference`),
#                                                                 KEY `message_references_idx_messageid` (`messageid`),
#                                                                     CONSTRAINT `FK_message_references_message_ids` FOREIGN KEY (`messageid`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
#                                                                                                                                                                                                          CONSTRAINT `FK_message_references_message_ids_2` FOREIGN KEY (`reference`) REFERENCES `message_ids` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
# ua_scienceCREATE TABLE IF NOT EXISTS `message_subject_lines` (
#     `id` int(11) NOT NULL AUTO_INCREMENT,
#                           `subject` mediumtext NOT NULL,
#                                                    PRIMARY KEY (`id`),
#                                                            KEY `message_subject_line_idx_id` (`id`),
#                                                                FULLTEXT KEY `subject` (`subject`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
# CREATE TABLE IF NOT EXISTS `newsgroup_ids` (
#     `id` int(11) NOT NULL AUTO_INCREMENT,
#                           `newsgroupname` varchar(255) NOT NULL,
#                                                            PRIMARY KEY (`id`),
#                                                                    UNIQUE KEY `newsgroupname` (`newsgroupname`),
#                                                                               KEY `newsgroupname-index` (`newsgroupname`),
#                                                                                   KEY `newsgroup_ids_idx_id` (`id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#
