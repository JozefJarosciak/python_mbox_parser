###########################
#  GOOGLE GROUPS PARSER   #
# Author: Jozef Jarosciak #
# Email: jarosciak@gmail  #
# License: MIT            #
###########################
from datetime import timedelta

import configuration
import dateutil.parser
import requests
from bs4 import BeautifulSoup
import sys


def round_down(num, divisor):
    return num - (num % divisor)


def get_available_group():
    try:
        sql = f"select newsgroup_name from all_messages.__all_files where newsgroup_name not in (select newsgroup_name from all_messages.__all_files group by newsgroup_name having max(processing) = 1 or max(ggdone) = 1) and  total > 10000 and current > 10000 and newsgroup_name not like '%+%' and newsgroup_name not like 'alt.%' group by newsgroup_name"
        db_cursor.execute(sql)
        details = db_cursor.fetchone()
        return details[0]
    except Exception:
        return str(sys.argv[1])
        pass


parsed_date = None
page_step = 100
count_processed_pages: int = 0
userAgent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'}
countThreads = 0
countSuccessfullMsgs = 0
countFailedMsgs = 0
countBeforeExit = 0
page = None
file_name = None

nntp_connection = configuration.nntp_connection
db_cursor = configuration.db_connection.cursor()

groupToParse = None

try:
    groupToParse = str(sys.argv[1])
except Exception:
    pass

if groupToParse is None:
    groupToParse = get_available_group()

while len(groupToParse) > 1:
    if groupToParse is None:
        groupToParse = get_available_group()

    print("Processing group:", groupToParse)


    try:
        sql = f"SELECT count(distinct(thread)) FROM all_messages.__all_google where groupname='{groupToParse}'"
        db_cursor.execute(sql)
        details = db_cursor.fetchone()
        countThreads = round_down(round(int(details[0])), page_step)
        print("So far synced: ", countThreads, " threads")
    except Exception:
        pass

    if countThreads > 0:
        sql = f"select thread from all_messages.__all_google where groupname='{groupToParse}' order by timestamp desc limit 1"
        db_cursor.execute(sql)
        details = db_cursor.fetchone()
        lastThread = str(details[0])
        # remove last processed thread (to clear unprocessed messages during crash
        sql = f"DELETE FROM all_messages.__all_google WHERE groupname='{groupToParse}' and thread='{lastThread}'"
        db_cursor.execute(sql)
        print("Removed last thread: ", lastThread)


    group_name_fin_db = groupToParse.replace(".", "_").replace("-", "_").replace("+", "_")
    if len(group_name_fin_db) > 45:
        group_name_fin_db = group_name_fin_db[-45:]

    sql = f"SELECT MIN(dateparsed) FROM all_messages.{group_name_fin_db}_headers"
    db_cursor.execute(sql)
    details = db_cursor.fetchone()
    minDate = details[0]
    minDateAdj = minDate - timedelta(days=7)
    print("Processing will stop at date > ", minDate)

    soup = BeautifulSoup(requests.get('https://groups.google.com/forum/?_escaped_fragment_=forum/' + groupToParse, headers=userAgent).text, 'html.parser')
    last_page_google = int(str(soup.find('i').text).replace("Showing 1-20 of ", "").replace(" topics", "").replace("Se muestran 1-20 de ", "").replace(" temas", "").replace(".", ""))
    print("Total Pages at Google:", last_page_google)
    last_page = round_down(last_page_google, page_step) + page_step
    last_page = last_page - countThreads

    for i in range(last_page, 0, -page_step):
        count_processed_pages += 1
        # if count_processed_pages == 5:
        #   exit(0)
        start = i - (page_step - 1)
        end = i

        page = "https://groups.google.com/forum/?_escaped_fragment_=forum/" + groupToParse + "[" + str(start) + "-" + str(end) + "]"
        soupPage = BeautifulSoup(requests.get(page, headers=userAgent).text, 'html.parser')
        print("Page: ", start, end, page)

        for tr in soupPage.find_all('tr'):

            #############################
            # FIND EACH DISCUSSION CODE #
            #############################
            try:
                link = str(tr.find('a', href=True)['href'])
                date = str(tr.find(class_="lastPostDate").text)
            except Exception:
                continue
            parsed_date = dateutil.parser.parse(date)
            # Break if the date of the message is newer than what's in the DB
            if parsed_date > minDate:
                print("---------------------------------------------------------------------------")
                print(countBeforeExit, "Group ", groupToParse, " was successfully parsed, up to: ", parsed_date)
                countBeforeExit += 1
                if countBeforeExit > 100:
                    file_name = None
                    try:
                        file_name = groupToParse + ".googlegroups"
                        # db_cursor.close()
                        sql = f"UPDATE all_messages.__all_files SET ggdone=1  WHERE newsgroup_name = '{groupToParse}'"
                        # print(sql)tstamp
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql)
                        configuration.db_connection.commit()
                        # db_cursor.close()
                    except Exception as err:
                        pass
                    exit(0)

            extractedDiscussionCode = link.replace('https://groups.google.com/d/topic/', '').replace(groupToParse + '/', '')

            # Mark in DB as being processed
            file_name = None
            try:
                file_name = groupToParse + ".googlegroups"
                # db_cursor.close()
                sql = f"INSERT INTO all_messages.__all_files(file_name, current, total, processing, newsgroup_name,tstamp) VALUES ('{file_name}',{i},{last_page_google},1,'{groupToParse}',now()) ON CONFLICT (file_name) DO UPDATE SET current={i}, total={last_page_google}, processing=1,tstamp = now()"
                # print(sql)tstamp
                # db_cursor = configuration.db_connection.cursor()
                db_cursor.execute(sql)
                configuration.db_connection.commit()
                # db_cursor.close()
            except Exception as err:
                # print("Exception #: 22")
                # db_cursor.close()
                exit()

            query = f"select count(*) from all_messages.__all_google where thread = '{extractedDiscussionCode}'"
            db_cursor.execute(query)
            discussionExist = db_cursor.fetchone()[0]
            if discussionExist == 0:
                ##############################
                # FIND CODE OF EACH MESSAGE  #
                ##############################
                page = f'https://groups.google.com/forum/?_escaped_fragment_=topic/{groupToParse}/' + extractedDiscussionCode
                print("\t", "Discussion: ", str(parsed_date.date()), page)
                soupDiscussion = BeautifulSoup(requests.get(page, headers=userAgent).text, 'html.parser')
                countMsgsInDiscussion = 0
                for tr1 in soupDiscussion.find_all('tr'):
                    countMsgsInDiscussion += 1
                    try:
                        # if message is not hidden
                        td = tr1.find(class_="subject")
                        if td is None:
                            break
                        linkMsg = str(td.find('a', href=True)['href'])
                        dateMsg = str(tr1.find(class_="lastPostDate").text)
                        parsed_dateMsg = dateutil.parser.parse(dateMsg)
                    except Exception:
                        # if message is hidden
                        td = tr1.find(class_="snippet")
                        linkMsg = str(td.find('a', href=True)['href'])



                    #######################
                    # GET THE RAW MESSAGE #
                    #######################
                    extractedMsgCode = linkMsg.replace(f'https://groups.google.com/d/msg/{groupToParse}/' + extractedDiscussionCode + '/', '')
                    try:
                        page = f'https://groups.google.com/forum/message/raw?msg={groupToParse}/' + extractedDiscussionCode + '/' + extractedMsgCode
                        soupRawMsg = BeautifulSoup(requests.get(page, headers=userAgent).text, 'html.parser')
                        rawMsg = soupRawMsg.text
                        if len(rawMsg) > 10:
                            countSuccessfullMsgs += 1
                            if countSuccessfullMsgs > 50:
                                countFailedMsgs = 0
                                countSuccessfullMsgs = 0
                            print("\t\t", "MSG: ", countMsgsInDiscussion, parsed_dateMsg, page)
                            # enter message into DB
                            try:
                                sql = f" INSERT INTO all_messages.__all_google (groupname, msg, thread, msgtime, body, timestamp) VALUES ((%s), (%s), (%s), (%s), (%s), DEFAULT)"
                                # db_cursor = configuration.db_connection.cursor()
                                db_cursor.execute(sql, (groupToParse, extractedMsgCode, extractedDiscussionCode, parsed_dateMsg, rawMsg))
                                configuration.db_connection.commit()
                            except Exception as ee:
                                print(ee)
                                print("Failed to enter into DB: ", extractedMsgCode)
                                pass
                        else:
                            print("\t\t", "MSG: ", extractedMsgCode, parsed_dateMsg, " - NOT Parsed")
                            countFailedMsgs += 1
                            if countFailedMsgs > page_step:
                                print("Failed: ", countFailedMsgs, extractedMsgCode)
                                exit(0)
                    except Exception as e:
                        print(e)
                        rawMsg = ''
                        print("\t\t", "MSG: ", extractedMsgCode, parsed_dateMsg, " - NOT Parsed")
                        countFailedMsgs += 1
                        if countFailedMsgs > 10:
                            exit(0)
            else:
                pass
                # print("\t\t", "THREAD: ", extractedDiscussionCode, parsed_date, " - SKIPPED")
