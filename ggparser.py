###########################
#  GOOGLE GROUPS PARSER   #
# Author: Jozef Jarosciak #
# Email: jarosciak@gmail  #
# License: MIT            #
###########################

import configuration
import dateutil.parser
import requests
from bs4 import BeautifulSoup
import sys


def round_down(num, divisor):
    return num - (num % divisor)


parsed_date = None
page_step = 100
count_processed_pages: int = 0
userAgent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'}
countThreads = 0

nntp_connection = configuration.nntp_connection
db_cursor = configuration.db_connection.cursor()

groupToParse = str(sys.argv[1])
print("Processing group:", groupToParse)

try:
    sql = f"SELECT count(distinct(thread)) FROM all_messages.__all_google where groupname='{groupToParse}'"
    db_cursor.execute(sql)
    details = db_cursor.fetchone()
    countThreads = round_down(round(int(details[0])), page_step)
    print(countThreads)
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
    print("Page: ", start, end)
    soupPage = BeautifulSoup(requests.get("https://groups.google.com/forum/?_escaped_fragment_=forum/" + groupToParse + "[" + str(start) + "-" + str(end) + "]", headers=userAgent).text, 'html.parser')

    for tr in soupPage.find_all('tr'):

        #############################
        # FIND EACH DISCUSSION CODE #
        #############################
        link = str(tr.find('a', href=True)['href'])
        date = str(tr.find(class_="lastPostDate").text)
        parsed_date = dateutil.parser.parse(date)
        extractedDiscussionCode = link.replace('https://groups.google.com/d/topic/', '').replace(groupToParse + '/', '')

        query = f"select count(*) from all_messages.__all_google where thread = '{extractedDiscussionCode}'"
        db_cursor.execute(query)
        discussionExist = db_cursor.fetchone()[0]
        if discussionExist == 0:
            ##############################
            # FIND CODE OF EACH MESSAGE  #
            ##############################
            print("\t", "Discussion: ", extractedDiscussionCode, str(parsed_date.date()))
            soupDiscussion = BeautifulSoup(requests.get('https://groups.google.com/forum/?_escaped_fragment_=topic/comp.lang.c/' + extractedDiscussionCode, headers=userAgent).text, 'html.parser')
            countMsgsInDiscussion = 0
            for tr1 in soupDiscussion.find_all('tr'):
                countMsgsInDiscussion += 1
                td = tr1.find(class_="subject")
                linkMsg = str(td.find('a', href=True)['href'])
                dateMsg = str(tr1.find(class_="lastPostDate").text)
                parsed_dateMsg = dateutil.parser.parse(dateMsg)
                # Break if the date of the message is newer than what's in the DB
                if parsed_dateMsg > minDate:
                    print("---------------------------------------------------------------------------")
                    print("Group ", groupToParse, " was successfully parsed, up to: ", parsed_dateMsg)
                    exit(0)

                #######################
                # GET THE RAW MESSAGE #
                #######################
                extractedMsgCode = linkMsg.replace('https://groups.google.com/d/msg/comp.lang.c/' + extractedDiscussionCode + '/', '')
                try:
                    soupRawMsg = BeautifulSoup(requests.get('https://groups.google.com/forum/message/raw?msg=comp.lang.c/' + extractedDiscussionCode + '/' + extractedMsgCode, headers=userAgent).text, 'html.parser')
                    rawMsg = soupRawMsg.text
                    if len(rawMsg) > 10:
                        print("\t\t", "MSG: ", extractedMsgCode, parsed_dateMsg, " - PARSED")
                        # enter message into DB
                        sql = f" INSERT INTO all_messages.__all_google (groupname, msg, thread, msgtime, body, timestamp) VALUES ((%s), (%s), (%s), (%s), (%s), DEFAULT)"
                        # db_cursor = configuration.db_connection.cursor()
                        db_cursor.execute(sql, (groupToParse, extractedMsgCode, extractedDiscussionCode, parsed_dateMsg, rawMsg))
                        configuration.db_connection.commit()
                    else:
                        print("\t\t", "MSG: ", extractedMsgCode, parsed_dateMsg, " - NOT Parsed")
                        exit(0)
                except Exception:
                    print(Exception)
                    rawMsg = ''
                    print("\t\t", "MSG: ", extractedMsgCode, parsed_dateMsg, " - NOT Parsed")
                    exit(0)
        else:
            pass
            # print("\t\t", "THREAD: ", extractedDiscussionCode, parsed_date, " - SKIPPED")
