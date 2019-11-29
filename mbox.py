import datetime
import glob
import gzip
import json
import mailbox
import re
import shutil

import mailbox as mailbox
import mysql.connector
import mailparser
import sys
import time

# Connect to MySQL
print("Connecting MYSQL DB")
try:
    # Imports Credentials for MySQL    #
    import mysql_database # Importing local mysql_database.py file
    db_cursor = mysql_database.db_connection.cursor()
    print("DB Connected: " + str(mysql_database.db_connection.fetch_eof_status()))
except ImportError:
    print('Importing mysql_database.py file failed. Create mysql_database.py file and add to it: '
          'mysql_database = mysql.connector.connect(host="localhost", user="Your MySQL Username", passwd="Your MySQL Password")')


path = 'D:/GiganewsArchives/giganews/downloads/usenet-0.akita-inu/'
print("Processing all files on path: " + str(path))

files = [f for f in glob.glob(path + '**/*.mbox.gz', recursive=True)]
count = 0
for f in files:
    f = f.replace("\\", "/")
    filename = f.replace(path, "").replace(".gz", "")
    count = count + 1
    # Unzip MBOX.GZ and Place to TMP
    print("Starting to Unzip: " + str(count) + " - " + str(f))
    with gzip.open(f, 'rb') as f_in:
        # with open('C:/tmp/'+str(count)+'-'+str(filename), 'wb') as f_out:
        where2unzip = 'C:/tmp/temp.mbox'
        with open(where2unzip, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("Unzipped to: " + where2unzip)
    print("Starting to Process MBOX")
    mbox = mailbox.mbox(where2unzip)

    i = 0

    for message in mbox:
        i = i + 1
        print(" -- Processing message: " + str(i))
        try:
            parsed_message = mailparser.parse_from_string(str(message))

            # Insert Message ID into message_ids table
            sql = f"SELECT * FROM usenetarchive.message_ids WHERE messageid = '{message['message-id']}' LIMIT 1"
            db_cursor.execute(sql) ; db_cursor.fetchall() ; number_of_rows = db_cursor.rowcount
            if number_of_rows == 0:
                sql = "INSERT INTO usenetarchive.message_ids(messageid) VALUE (%s)"
                db_cursor.execute(sql, (message['message-id'],))
                mysql_database.commit()
            # get last insert ID
            db_cursor.execute(f"SELECT id FROM usenetarchive.message_ids WHERE messageid = '{message['message-id']}'")
            sql_message_id = db_cursor.fetchone()

            # Insert From ID into from_contacts table
            message_from_name = parsed_message.from_[0][0]
            message_from_email = parsed_message.from_[0][1].lower()
            sql = f"SELECT * FROM usenetarchive.from_contacts WHERE from_email = '{message_from_email}' LIMIT 1"
            db_cursor.execute(sql) ; db_cursor.fetchall() ; number_of_rows = db_cursor.rowcount
            if number_of_rows == 0:
                sql = "INSERT INTO usenetarchive.from_contacts(from_name, from_email) VALUES ((%s),(%s))"
                db_cursor.execute(sql, (message_from_name, message_from_email,))
                mysql_database.commit()
            # get last insert ID
            db_cursor.execute(f"SELECT id FROM usenetarchive.from_contacts WHERE from_email = '{message_from_email}'")
            sql_from_id = db_cursor.fetchone()

            # Insert Newsgroup Name into newsgroup_ids table
            message_newsgroup_ids = re.sub('\s+', '', message['newsgroups'])
            newsgroup_names_array = message_newsgroup_ids.split(',')
            for group_name in newsgroup_names_array:

                sql = f"SELECT * FROM usenetarchive.newsgroup_ids WHERE newsgroupname = '{group_name}' LIMIT 1"
                db_cursor.execute(sql) ; db_cursor.fetchall() ; number_of_rows = db_cursor.rowcount
                if number_of_rows == 0:
                    sql = "INSERT INTO usenetarchive.newsgroup_ids(newsgroupname) VALUE (%s)"
                    db_cursor.execute(sql, (group_name,))
                    mysql_database.commit()
                # get last insert ID
                db_cursor.execute(f"SELECT id FROM usenetarchive.newsgroup_ids WHERE newsgroupname = '{group_name}'")
                sql_newsgroup_id = db_cursor.fetchone()

                # Update all_messages table with references to message id and newsgroup id
                sql = f"SELECT * FROM usenetarchive.all_messages WHERE messageid={str(sql_message_id[0])} AND newsgroup={str(sql_newsgroup_id[0])} AND from_contact={str(sql_from_id[0])}"
                db_cursor.execute(sql)
                db_cursor.fetchall()
                number_of_rows = db_cursor.rowcount
                if number_of_rows == 0:
                    sql = f"INSERT INTO usenetarchive.all_messages (messageid, newsgroup, from_contact) VALUES ({sql_message_id[0]},{sql_newsgroup_id[0]},{sql_from_id[0]})"
                    db_cursor.execute(sql)
                    mysql_database.commit()





        #           sql = "INSERT INTO usenetarchive.message(postdate, jsondata) VALUES (%s,%s)"
        #           db_cursor.execute(sql, (parsed_message.date, parsed_message.mail_json,))
        #           db_connection.commit()
        #           print(db_cursor.rowcount, "Record Inserted")
        except Exception as inst:
            print(inst)
            exit()

        if i == 2: exit(0)

    print("**************************************")

    #

# newsgroup_ids TABLE
# CREATE TABLE `newsgroup_ids` (
#     `id` INT(11) NOT NULL AUTO_INCREMENT,
#                           `newsgroupname` VARCHAR(255) NULL DEFAULT NULL,
#                                                                     PRIMARY KEY (`id`),
#                                                                             UNIQUE INDEX `newsgroupname` (`newsgroupname`)
# )
# COLLATE='utf8mb4_0900_ai_ci'
# ENGINE=InnoDB
# ;
# INSERT INTO `usenetarchive`.`newsgroup_ids` (`newsgroupname`) VALUES ('0.akita-inu');
