###########################
#   UTZOO to PostgreSQL   #
#      Python Parser      #
# Author: Jozef Jarosciak #
# Email: jarosciak@gmail  #
# License: MIT            #
#########################################################
# Details at: https://www.joe0.com/?p=4678&preview=true #
#########################################################

from pathlib import Path
import dateutil.parser
import base64
import email
import quopri
import random
import re
import string
from datetime import date
import cchardet
import dateutil.parser
from bs4 import UnicodeDammit
import psycopg2
import os

# define DB connection
try:
    db_connection = psycopg2.connect(host="localhost", user="postgres", password="", port="5432", database="utzoo")
except Exception as e:
    print(e)
    exit(0)

# define path to un-tared Utzoo archive
# for Windows
positionFilePath = "E:\\Usenet\\Utzoo\\"
# for linux:
# positionFilePath = "/home/utzoo/Utzoo/"

timezone_info = {
    "A": 1 * 3600,
    "ACDT": 10.5 * 3600,
    "ACST": 9.5 * 3600,
    "ACT": -5 * 3600,
    "ACWST": 8.75 * 3600,
    "ADT": 4 * 3600,
    "AEDT": 11 * 3600,
    "AEST": 10 * 3600,
    "AET": 10 * 3600,
    "AFT": 4.5 * 3600,
    "AKDT": -8 * 3600,
    "AKST": -9 * 3600,
    "ALMT": 6 * 3600,
    "AMST": -3 * 3600,
    "AMT": -4 * 3600,
    "ANAST": 12 * 3600,
    "ANAT": 12 * 3600,
    "AQTT": 5 * 3600,
    "ART": -3 * 3600,
    "AST": 3 * 3600,
    "AT": -4 * 3600,
    "AWDT": 9 * 3600,
    "AWST": 8 * 3600,
    "AZOST": 0 * 3600,
    "AZOT": -1 * 3600,
    "AZST": 5 * 3600,
    "AZT": 4 * 3600,
    "AoE": -12 * 3600,
    "B": 2 * 3600,
    "BNT": 8 * 3600,
    "BOT": -4 * 3600,
    "BRST": -2 * 3600,
    "BRT": -3 * 3600,
    "BST": 6 * 3600,
    "BTT": 6 * 3600,
    "C": 3 * 3600,
    "CAST": 8 * 3600,
    "CAT": 2 * 3600,
    "CCT": 6.5 * 3600,
    "CDT": -5 * 3600,
    "CEST": 2 * 3600,
    "CET": 1 * 3600,
    "CHADT": 13.75 * 3600,
    "CHAST": 12.75 * 3600,
    "CHOST": 9 * 3600,
    "CHOT": 8 * 3600,
    "CHUT": 10 * 3600,
    "CIDST": -4 * 3600,
    "CIST": -5 * 3600,
    "CKT": -10 * 3600,
    "CLST": -3 * 3600,
    "CLT": -4 * 3600,
    "COT": -5 * 3600,
    "CST": -6 * 3600,
    "CT": -6 * 3600,
    "CVT": -1 * 3600,
    "CXT": 7 * 3600,
    "ChST": 10 * 3600,
    "D": 4 * 3600,
    "DAVT": 7 * 3600,
    "DDUT": 10 * 3600,
    "E": 5 * 3600,
    "EASST": -5 * 3600,
    "EAST": -6 * 3600,
    "EAT": 3 * 3600,
    "ECT": -5 * 3600,
    "EDT": -4 * 3600,
    "EEST": 3 * 3600,
    "EET": 2 * 3600,
    "EGST": 0 * 3600,
    "EGT": -1 * 3600,
    "EST": -5 * 3600,
    "ET": -5 * 3600,
    "F": 6 * 3600,
    "FET": 3 * 3600,
    "FJST": 13 * 3600,
    "FJT": 12 * 3600,
    "FKST": -3 * 3600,
    "FKT": -4 * 3600,
    "FNT": -2 * 3600,
    "G": 7 * 3600,
    "GALT": -6 * 3600,
    "GAMT": -9 * 3600,
    "GET": 4 * 3600,
    "GFT": -3 * 3600,
    "GILT": 12 * 3600,
    "GMT": 0 * 3600,
    "GST": 4 * 3600,
    "GYT": -4 * 3600,
    "H": 8 * 3600,
    "HDT": -9 * 3600,
    "HKT": 8 * 3600,
    "HOVST": 8 * 3600,
    "HOVT": 7 * 3600,
    "HST": -10 * 3600,
    "I": 9 * 3600,
    "ICT": 7 * 3600,
    "IDT": 3 * 3600,
    "IOT": 6 * 3600,
    "IRDT": 4.5 * 3600,
    "IRKST": 9 * 3600,
    "IRKT": 8 * 3600,
    "IRST": 3.5 * 3600,
    "IST": 5.5 * 3600,
    "JST": 9 * 3600,
    "K": 10 * 3600,
    "KGT": 6 * 3600,
    "KOST": 11 * 3600,
    "KRAST": 8 * 3600,
    "KRAT": 7 * 3600,
    "KST": 9 * 3600,
    "KUYT": 4 * 3600,
    "L": 11 * 3600,
    "LHDT": 11 * 3600,
    "LHST": 10.5 * 3600,
    "LINT": 14 * 3600,
    "M": 12 * 3600,
    "MAGST": 12 * 3600,
    "MAGT": 11 * 3600,
    "MART": 9.5 * 3600,
    "MAWT": 5 * 3600,
    "MDT": -6 * 3600,
    "MHT": 12 * 3600,
    "MMT": 6.5 * 3600,
    "MSD": 4 * 3600,
    "MSK": 3 * 3600,
    "MST": -7 * 3600,
    "MT": -7 * 3600,
    "MUT": 4 * 3600,
    "MVT": 5 * 3600,
    "MYT": 8 * 3600,
    "N": -1 * 3600,
    "NCT": 11 * 3600,
    "NDT": 2.5 * 3600,
    "NFT": 11 * 3600,
    "NOVST": 7 * 3600,
    "NOVT": 7 * 3600,
    "NPT": 5.5 * 3600,
    "NRT": 12 * 3600,
    "NST": 3.5 * 3600,
    "NUT": -11 * 3600,
    "NZDT": 13 * 3600,
    "NZST": 12 * 3600,
    "O": -2 * 3600,
    "OMSST": 7 * 3600,
    "OMST": 6 * 3600,
    "ORAT": 5 * 3600,
    "P": -3 * 3600,
    "PDT": -7 * 3600,
    "PET": -5 * 3600,
    "PETST": 12 * 3600,
    "PETT": 12 * 3600,
    "PGT": 10 * 3600,
    "PHOT": 13 * 3600,
    "PHT": 8 * 3600,
    "PKT": 5 * 3600,
    "PMDT": -2 * 3600,
    "PMST": -3 * 3600,
    "PONT": 11 * 3600,
    "PST": -8 * 3600,
    "PT": -8 * 3600,
    "PWT": 9 * 3600,
    "PYST": -3 * 3600,
    "PYT": -4 * 3600,
    "Q": -4 * 3600,
    "QYZT": 6 * 3600,
    "R": -5 * 3600,
    "RET": 4 * 3600,
    "ROTT": -3 * 3600,
    "S": -6 * 3600,
    "SAKT": 11 * 3600,
    "SAMT": 4 * 3600,
    "SAST": 2 * 3600,
    "SBT": 11 * 3600,
    "SCT": 4 * 3600,
    "SGT": 8 * 3600,
    "SRET": 11 * 3600,
    "SRT": -3 * 3600,
    "SST": -11 * 3600,
    "SYOT": 3 * 3600,
    "T": -7 * 3600,
    "TAHT": -10 * 3600,
    "TFT": 5 * 3600,
    "TJT": 5 * 3600,
    "TKT": 13 * 3600,
    "TLT": 9 * 3600,
    "TMT": 5 * 3600,
    "TOST": 14 * 3600,
    "TOT": 13 * 3600,
    "TRT": 3 * 3600,
    "TVT": 12 * 3600,
    "U": -8 * 3600,
    "ULAST": 9 * 3600,
    "ULAT": 8 * 3600,
    "UTC": 0 * 3600,
    "UYST": -2 * 3600,
    "UYT": -3 * 3600,
    "UZT": 5 * 3600,
    "V": -9 * 3600,
    "VET": -4 * 3600,
    "VLAST": 11 * 3600,
    "VLAT": 10 * 3600,
    "VOST": 6 * 3600,
    "VUT": 11 * 3600,
    "W": -10 * 3600,
    "WAKT": 12 * 3600,
    "WARST": -3 * 3600,
    "WAST": 2 * 3600,
    "WAT": 1 * 3600,
    "WEST": 1 * 3600,
    "WET": 0 * 3600,
    "WFT": 12 * 3600,
    "WGST": -2 * 3600,
    "WGT": -3 * 3600,
    "WIB": 7 * 3600,
    "WIT": 9 * 3600,
    "WITA": 8 * 3600,
    "WST": 14 * 3600,
    "WT": 0 * 3600,
    "X": -11 * 3600,
    "Y": -12 * 3600,
    "YAKST": 10 * 3600,
    "YAKT": 9 * 3600,
    "YAPT": 10 * 3600,
    "YEKST": 6 * 3600,
    "YEKT": 5 * 3600,
    "Z": 0 * 3600
}

today = date.today()
print("** START **")
print("Starting at:", today)

db_cursor = db_connection.cursor()
processing_message_counter = dict()
counterall = 0
last_page = 0


def convert_encoding(data, new_coding='UTF-8'):
    encoding = cchardet.detect(data)['encoding']
    if new_coding.upper() != encoding.upper():
        data = data.decode(encoding, data).encode(new_coding)
    return data


insertedMsgs = 0

completeProcessing = 0

positionFilePath = positionFilePath + "counter.txt"

for path in Path(positionFilePath.replace("\\counter.txt", "").replace("/counter.txt", "")).rglob('*'):
    if os.path.isfile(path):
        counterall = counterall + 1

    if os.path.exists(positionFilePath):
        with open(positionFilePath, 'r') as file:
            last_page = int(file.read().replace('\n', ''))

    if last_page > counterall:
        continue

    if os.path.isfile(path):
        if ".TARDIRPERMS_" in str(path):
            continue

        ggmsg = None
        ggthread = None
        groupName = None
        body = None
        message = None

        try:
            message_from_utzoo_file = Path(path.absolute()).read_text()
        except Exception as e:
            print(path.absolute())
            message_from_utzoo_file = Path(path.absolute()).read_text(encoding="ascii", errors="ignore")

        try:
            message = email.message_from_string(message_from_utzoo_file)
            if message['Newsgroups']:
                # print(message)
                pass
            else:
                # print("Broken No Newsgroup: ", path)
                continue
        except Exception as e:
            continue

        if ("," in str(message['Newsgroups'])) or (" " in str(message['Newsgroups'])):
            groupName = str(message['Newsgroups']).split(",")
            groupName = str(groupName[0]).split(" ")
            groupName = groupName[0]
        else:
            groupName = message['Newsgroups']

        body = message.get_payload()

        if groupName is None:
            completeProcessing = 1
            print("Processed all messages")
            exit(0)

        # print(groupName, ggmsg)
        group_name_fin_db = groupName.replace(".", "_").replace("-", "_").replace("+", "_")
        if len(group_name_fin_db) > 45:
            group_name_fin_db = group_name_fin_db[-45:]

        # Create tables for a new group
        db_cursor = db_connection.cursor()
        db_cursor.execute(
            f"select exists(select * from information_schema.tables where table_name='{group_name_fin_db}_headers')")
        exist = db_cursor.fetchone()[0]
        # db_cursor.close()

        if not exist:

            try:
                sql = f"create table all_messages.{group_name_fin_db}_headers(id bigserial not null constraint {group_name_fin_db}_headers_pk primary key, dateparsed timestamp, subj_id bigint, ref smallint, msg_id text, msg_from bigint, enc text, contype text, processed timestamp default CURRENT_TIMESTAMP);alter table all_messages.{group_name_fin_db}_headers owner to postgres;"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql)
                db_connection.commit()
                # db_cursor.close()

                sql = f"create table all_messages.{group_name_fin_db}_refs(id bigint, ref_msg text default null);alter table all_messages.{group_name_fin_db}_refs owner to postgres;"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql)
                db_connection.commit()
                # db_cursor.close()

                sql = f"create table all_messages.{group_name_fin_db}_body(id bigint primary key, data text default null);alter table all_messages.{group_name_fin_db}_body owner to postgres;"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql)
                db_connection.commit()
                # db_cursor.close()

                sql = f"create table all_messages.{group_name_fin_db}_from(id serial not null constraint {group_name_fin_db}_from_pk primary key, data text);alter table all_messages.{group_name_fin_db}_from owner to postgres;"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql)
                db_connection.commit()
                # db_cursor.close()

                sql = f"create table all_messages.{group_name_fin_db}_subjects(id serial not null constraint {group_name_fin_db}_subjects_pk primary key, subject text);alter table all_messages.{group_name_fin_db}_subjects owner to postgres;"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql)
                db_connection.commit()
                # db_cursor.close()

                sql = f"create unique index {group_name_fin_db}_headers_uiidx on all_messages.{group_name_fin_db}_headers(id);" \
                      f"create unique index {group_name_fin_db}_headers_umidx on all_messages.{group_name_fin_db}_headers(msg_id);" \
                      f"create unique index {group_name_fin_db}_body_idx on all_messages.{group_name_fin_db}_body(id);" \
                      f"create unique index {group_name_fin_db}_from_idx on all_messages.{group_name_fin_db}_from(data);" \
                      f"create unique index {group_name_fin_db}_subjects_idx on all_messages.{group_name_fin_db}_subjects(subject);"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql)
                db_connection.commit()
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

        # DATA CLEAN UP - DATE

        if parsed_subject is None:
            parsed_subject = message['Title']

        # if "Re:" in parsed_subject:
        #   print(message.keys())
        #   print(parsed_subject)

        if parsed_date:
            #parsed_date = parsed_date.replace("Wednesday, ", "")
            #print(parsed_date)
            try:
                parsed_date_check = dateutil.parser.parse(parsed_date, tzinfos=timezone_info)
            except Exception as e:
                try:
                    parsed_date_check = dateutil.parser.parse(parsed_date.upper(), tzinfos=timezone_info)
                except Exception as e:
                    parsed_date_check = None

        if parsed_date_check is None or (parsed_date_check.hour == 0 and parsed_date_check.minute == 0 and parsed_date_check.second == 0 and parsed_date_check.microsecond == 0):
            parsed_date = message['NNTP-Posting-Date']
            if parsed_date:
                parsed_date_check = dateutil.parser.parse(parsed_date, tzinfos=timezone_info)
        if parsed_date_check is None or (parsed_date_check.hour == 0 and parsed_date_check.minute == 0 and parsed_date_check.second == 0 and parsed_date_check.microsecond == 0):
            parsed_date = message['X-Article-Creation-Date']
            if parsed_date:
                parsed_date_check = dateutil.parser.parse(parsed_date, tzinfos=timezone_info)
        if parsed_date_check is None or (parsed_date_check.hour == 0 and parsed_date_check.minute == 0 and parsed_date_check.second == 0 and parsed_date_check.microsecond == 0):
            parsed_date = message['Posted']
            if parsed_date:
                #print(parsed_date)
                parsed_date_check = dateutil.parser.parse(parsed_date.split('(')[0], tzinfos=timezone_info)
        if parsed_date_check is None or (parsed_date_check.hour == 0 and parsed_date_check.minute == 0 and parsed_date_check.second == 0 and parsed_date_check.microsecond == 0):
            parsed_date = message['Received']
            if parsed_date:
                parsed_date_check = dateutil.parser.parse(parsed_date, tzinfos=timezone_info)

        if parsed_date is None:
            print('No date')
            print(message._headers)
            continue
            # exit(0)

        if parsed_message_id is None:
            parsed_message_id = message['Article-I.D.']

        try:
            if '(' in parsed_date:
                parsed_date = message['date'].split("(")[0].strip()
            else:
                parsed_date = message['date'].strip()
        except Exception:
            pass

        failing_zones_to_check = ['-13', '-14', '-15', '-16', '-17', '-18', '-19', '-20', '-21', '-22', '-23', '-24', '-25', '-26', '-27', '-28', '-29', '-30',
                                  '+15', '+16', '+17', '+18', '+19', '+20', '+21', '+22', '+23', '+24', '+25', '+26', '+27', '+28', '+29', '+30']
        try:
            for failedzone in failing_zones_to_check:
                if failedzone in parsed_date:
                    parsed_date = parsed_date.split(failedzone)[0]
                    print('Fixed: ' + parsed_date + ' | ' + failedzone)
                    break
            else:
                parsed_date = dateutil.parser.parse(parsed_date, tzinfos=timezone_info)
        except Exception:
            try:
                # Try to parse/convert NNTP-Posting-Date
                parsed_date = message['NNTP-Posting-Date']
                for failedzone in failing_zones_to_check:
                    if failedzone in parsed_date:
                        parsed_date = parsed_date.split(failedzone)[0]
                        print('Fixed NNTP: ' + parsed_date + ' | ' + failedzone)
                        break
                else:
                    parsed_date = dateutil.parser.parse(parsed_date, tzinfos=timezone_info)
            except Exception:
                # new_headers.append(tuple(("odate", value)))
                try:
                    print(" can't get date - skipping")
                    # #db_cursor.close()
                except Exception as err:
                    # print("Exception #: 22")
                    # #db_cursor.close()
                    exit()
                continue
                # exit(0)

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
            db_cursor = db_connection.cursor()
            parsed_message_id = removeNonAscii(parsed_message_id)
            query = f"select count(*) from all_messages.{group_name_fin_db}_headers where msg_id='" + parsed_message_id + "';"
            db_cursor.execute(query)
            msg_exist = db_cursor.fetchone()[0]
            # print("message_exists:")
            # print(msg_exist)
            # #db_cursor.close()
        except Exception as e:
            print(e)
            # print("Exception #: 9")
            # print(query)
            # #db_cursor.close()
            try:
                # Check If MSG ID already in db
                db_cursor = db_connection.cursor()

                query = f"select count(*) from all_messages.{group_name_fin_db}_headers where msg_id='{parsed_message_id}'"
                print(query)
                db_cursor.execute(query)
                msg_exist = db_cursor.fetchone()[0]
                # print("message_exists:")
                # print(msg_exist)
                # #db_cursor.close()
            except Exception as e:
                print("Passing: " + parsed_message_id)
                print(e)

                # print("Exception #: 10")
                # #db_cursor.close()
                msg_exist = 1
            pass

        # Continue only if MSG not in the headers db
        if msg_exist == 0:
            # try:
            try:
                # Add a unique subject line
                sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) RETURNING id"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql, (parsed_subject, parsed_subject))
                db_connection.commit()
                inserted_subject_id = db_cursor.fetchone()[0]
                # #db_cursor.close()
            except Exception as e:
                print(e)
                # print("Exception #: 11")
                # #db_cursor.close()
                # exit()
                if inserted_subject_id is None:
                    try:
                        parsed_subject = parsed_subject.encode("ascii", "ignore").decode()
                        parsed_subject = re.sub(r'[^\x00-\x7f]', r'', parsed_subject)
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) RETURNING id"
                        db_cursor = db_connection.cursor()
                        db_cursor.execute(sql, (parsed_subject, parsed_subject))
                        db_connection.commit()
                        inserted_subject_id = db_cursor.fetchone()[0]
                        # #db_cursor.close()
                    except Exception as e:
                        print(e)
                        # print("Exception #: 12")
                        # #db_cursor.close()
                        # exit()
                        try:
                            parsed_subject = re.sub(r'[^\x00-\x7f]', r'', parsed_subject_original)
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_subjects(subject) VALUES ((%s)) ON CONFLICT(subject) DO UPDATE SET subject=(%s) RETURNING id"
                            db_cursor = db_connection.cursor()
                            db_cursor.execute(sql, (parsed_subject, parsed_subject))
                            db_connection.commit()
                            inserted_subject_id = db_cursor.fetchone()[0]
                            # #db_cursor.close()
                        except Exception as e:
                            print(e)
                            # print("Exception #: 13")
                            # #db_cursor.close()
                            # exit()
                            pass

            try:
                # Add a unique from line
                sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES (%s) ON CONFLICT(data) DO UPDATE SET data=(%s) RETURNING id"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql, (parsed_from, parsed_from))
                db_connection.commit()
                inserted_from_id = db_cursor.fetchone()[0]
                # #db_cursor.close()
            except Exception as e:
                print(e)
                print(group_name_fin_db)
                # #db_cursor.close()
                if inserted_from_id is None:
                    try:
                        parsed_from = parsed_from.encode("ascii", "ignore").decode()
                        parsed_from = re.sub(r'[^\x00-\x7f]', r'', parsed_from)
                        # print("Exception #: 14")
                        print(parsed_from)
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES (%s) ON CONFLICT(data) DO UPDATE SET data=(%s) RETURNING id"
                        db_cursor = db_connection.cursor()
                        db_cursor.execute(sql, (parsed_from, parsed_from))
                        db_connection.commit()
                        inserted_from_id = db_cursor.fetchone()[0]
                        # #db_cursor.close()
                    except Exception:
                        try:
                            # #db_cursor.close()
                            parsed_from = re.sub(r'[^\x00-\x7f]', r'', parsed_from_original)
                            # print("Exception #: 15")
                            print(parsed_from)
                            sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES (%s) ON CONFLICT(data) DO UPDATE SET data=(%s) RETURNING id"
                            db_cursor = db_connection.cursor()
                            db_cursor.execute(sql, (parsed_from, parsed_from))
                            db_connection.commit()
                            inserted_from_id = db_cursor.fetchone()[0]
                            # #db_cursor.close()
                        except Exception:
                            try:
                                # #db_cursor.close()
                                parsed_from = re.search(r'<(.*?)>', parsed_from).group(1)
                                # print("Exception #: 16")
                                print(parsed_from)
                                sql = f"INSERT INTO all_messages.{group_name_fin_db}_from(data) VALUES ('{parsed_from}') ON CONFLICT(data) DO UPDATE SET data=('{parsed_from}') RETURNING id"
                                print(sql)
                                db_cursor = db_connection.cursor()
                                print("ss1")
                                db_cursor.execute(sql)
                                print("ss2")
                                db_connection.commit()
                                print("ss3")
                                inserted_from_id = db_cursor.fetchone()[0]
                                print(inserted_from_id)
                                # #db_cursor.close()
                            except Exception:
                                # print("Exception #: 17a")
                                # exit()
                                pass
            # Add a header info - pass in the subject line id from the previous statement
            #
            if not inserted_from_id:
                print("I couldn't get inserted_from_id!")
                print(path.absolute())
                print(message._headers)
                exit()

            if parsed_ref:
                has_ref = 1
            else:
                has_ref = 0

            try:
                sql = f"INSERT INTO all_messages.{group_name_fin_db}_headers(dateparsed, subj_id, ref, msg_id, msg_from, enc, contype) VALUES ((%s), (%s), (%s), (%s), (%s), (%s), (%s)) RETURNING id"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql, (
                    parsed_date, inserted_subject_id, has_ref, parsed_message_id, inserted_from_id, parsed_encoding,
                    parsed_content_type))
                db_connection.commit()
                inserted_header_id = db_cursor.fetchone()[0]
                # #db_cursor.close()
            except Exception as e:
                # print("Exception #: 16a")
                # #db_cursor.close()
                # exit()
                print(e)
                print('Duplicate MSG ID: ' + parsed_message_id)
                continue
                # exit(0)

            if parsed_ref:
                split_refs = parsed_ref.split(' ')
                for split in split_refs:
                    try:
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_refs(id, ref_msg) VALUES ((%s), (%s));"
                        db_cursor = db_connection.cursor()
                        db_cursor.execute(sql, (inserted_header_id, split.strip()))
                        db_connection.commit()
                        # #db_cursor.close()
                    except Exception:
                        # print("Exception #: 17")
                        # #db_cursor.close()
                        # exit()
                        pass
            try:
                sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                db_cursor = db_connection.cursor()
                db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                db_connection.commit()
                processing_message_counter[str(groupName)] = processing_message_counter.get(str(groupName), 0) + 1
                print(counterall, parsed_date, groupName, path, processing_message_counter.get(str(groupName), 0))
                # print(processing_message_counter.get(str(groupName), 0))
            except Exception:
                # print("Exception #: 18")
                # #db_cursor.close()
                try:
                    parsed_body_text = parsed_body_text.encode("ascii", "ignore").decode()
                    parsed_body_text = re.sub(r'[^\x00-\x7f]', r'', parsed_body_text)
                    sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                    db_cursor = db_connection.cursor()
                    db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                    db_connection.commit()
                    processing_message_counter[str(groupName)] = processing_message_counter.get(str(groupName), 0) + 1
                    print(counterall, parsed_date, groupName, path, processing_message_counter.get(str(groupName), 0))
                    # #db_cursor.close()
                except Exception:
                    # print("Exception #: 19")
                    # #db_cursor.close()
                    # parsed_body_text = parsed_body_text_original.encode('utf-8', 'surrogateescape').decode('ANSI')
                    try:
                        parsed_body_text = re.sub(r'[^\x00-\x7f]', r'', parsed_body_text)
                        sql = f"INSERT INTO all_messages.{group_name_fin_db}_body(id,data) VALUES ((%s), (%s))"
                        db_cursor = db_connection.cursor()
                        db_cursor.execute(sql, (inserted_header_id, parsed_body_text))
                        db_connection.commit()
                        processing_message_counter[str(groupName)] = processing_message_counter.get(str(groupName), 0) + 1
                        print(counterall, parsed_date, groupName, path, processing_message_counter.get(str(groupName), 0))
                        # #db_cursor.close()
                    except Exception:
                        # print("Exception #: 19")
                        # #db_cursor.close()
                        exit(0)
        else:
            processing_message_counter[str(groupName)] = processing_message_counter.get(str(groupName), 0) + 1
            print(counterall, "Skipping: ", groupName, path, processing_message_counter.get(str(groupName), 0))
            if "Re:" in parsed_subject:
                pass
                # print(parsed_subject)

        # counter
        if os.path.exists(positionFilePath):
            os.remove(positionFilePath)
            # print(positionFilePath, "removed")
        if not os.path.exists(positionFilePath):
            # print('Updated', positionFilePath)
            file = open(positionFilePath, 'w')
            file.write(str(counterall))
            file.close()