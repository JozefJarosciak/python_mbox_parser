import array
import nntplib

nntp_connection = nntplib.NNTP('news.giganews.com', user='e***', password='G***')
resp, count, first, last, name = nntp_connection.group('adobe.indesign.windows')
print('Group', name, 'has', count, 'articles, range', first, 'to', last)

resp, overviews = nntp_connection.over((first+5000, first+5200))
# (5, {'subject': 'Re: Nominate Hirokazu Yamamoto (oceancity) for commit privs.', 'from': 'Jeroen Ruigrok van der Werven <asmodai@in-nomine.org>', 'date': 'Mon, 11 Aug 2008 22:15:34 +0200', 'message-id': '<20080811201534.GL57679@nexus.in-nomine.org>', 'references': '<6167796BFEB5D0438720AC212E89A6B0078F4D64@exchange.onresolve.com> <48A0995D.6010902@v.loewis.de>', ':bytes': '5100', ':lines': '14', 'xref': 'news.gmane.org gmane.comp.python.committers:5'})
for id, over in overviews:
    body_decoded = ''
    try:
        resp, info = nntp_connection.body(over['message-id']) #nntplib.decode_header(nntp_connection.body(nntplib.decode_header(over['message-id'])))
        for line in info.lines:
            body_decoded += line.decode('utf-8')+'\n'
        print(id, nntplib.decode_header(over['date']), nntplib.decode_header(over['message-id']), nntplib.decode_header(over['subject']), nntplib.decode_header(over['references']))
        print(body_decoded)
    except Exception:
        print(f"{id} - NO BODY")
    print('+++++++++++++++++++++++++++++')