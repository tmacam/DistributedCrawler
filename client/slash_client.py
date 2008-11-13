#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Based on code in private repository:
#     client.py 243 2006-11-22 09:20:37 -0200 tmacam 
#

"""DistributedCrawling client for Slashdot.

See digg_client.py for a more up-to-date code regarding enviroment setup and
client execution.
"""

__version__ = "0.3.posdigg"
__date__ = '2006-11-22 09:20:37 -0200 (Wed, 22 Nov 2006)'
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'


import sys
import urllib2
import time
from socket import gethostname
from urllib import urlencode
import upload_aux
from BeautifulSoup import BeautifulSoup
from slash_article_retriever import ArticleRetriever, \
    NothingForYouToSeeErrorPage, __version__ as articleretriever_version
from daemonize import createDaemon, reconfigStdout
from client import BaseClient, getUUID, log_backtrace, log_urllib2_exception


# TODO(macambira): perhaps python has a nice logging framework now
log = sys.stdout


class SlashClient(BaseClient):
    """Client for crawling Slashdot Articles."""
    def __init__(self, client_id, base_url, store_dir=None):
        """SlashClient Constructor."""
        # Parent class constructor
        BaseClient.__init__(self, client_id, base_url, store_dir)
        # Informing the version of our ArticleRetriver
        self.headers["client-arver'"] =  articleretriever_version
        # Registering Command Handlers
        self.handlers['ISSUE'] = self.issue
        self.handlers['ARTICLE'] = self.article

    def _write_to_store(self, article_id, data):
        """Write a (compressed) article to store.
        
        Article ids is turned into something filesystem safe here.
        """
        safe_id = article_id.replace('/', '_')
        safe_id += '.html.gz'
        BaseClient._write_to_store(self, safe_id, data)

    def issue(self, params):
        """Download the list of articles published in a given day: an issue."""
        issue_url = "http://slashdot.org/index.pl?issue=" + params
        # Get and parse the issue
        log.write("ISSUE " + issue_url + "\n")
        data = urllib2.urlopen(issue_url).read()
        soup = BeautifulSoup(data)
        urls = [i.a['href'] for i in soup.findAll(['li', 'span'], attrs={'class':['more', 'storytitle']})]
        sids = [u.split('=')[1] for u in urls]
        log.write("ISSUE %s : %s \n" % (issue_url, " ".join(sids) ) )
        # Send the request back to the server w/ a HTTP POST request
        data = dict(self.headers)
        data['issue'] = params
        data['sids'] = " ".join(sids)
        req = urllib2.Request( self.base_url + '/issue/' + params,
                               urlencode(data), self.headers)
        fh = urllib2.urlopen(req)
        # Ok. Command, handled. Now what?
        # Do what the server told us to.
        # Command MUST be SLEEP. We will sleep for at least self.MIN_SLEEP
        command = fh.read()
        self._handleCommand(command, do_sleep=True)

    def article(self, params):
        """Get the article."""
        article_sid = params.strip()
        log.write("ARTICLE " + article_sid + " BEGIN\n")
        try:
            downloader = ArticleRetriever(article_sid, log=log)
            compressed_article = downloader.getArticleCompressed()
            self._write_to_store(article_sid, compressed_article)
            log.write("ARTICLE " + article_sid + " GOT COMPRESSED DATA\n")
            # Setup upload form and headers
            upload_headers = dict(self.headers)
            form_data = {'article-data' : compressed_article,
                         'article-sid'  : article_sid }
            # Upload the article
            upload_url = self.base_url + '/article/' + article_sid
            response = upload_aux.upload_form(upload_url,
                                              form_data,
                                              upload_headers)
        except NothingForYouToSeeErrorPage:
            log.write( "ARTICLE %s REPORTING NothingForYouToSee error.\n" %\
                            article_sid )
            # FIXME we need to report those pages to the server...
            # Well, for the moment, let's just pretend we didn't see
            # that command...
            req = urllib2.Request(self.base_url + '/nothing-error/' + \
                                    article_sid, headers=self.headers)
            response = urllib2.urlopen(req)
        log.write("ARTICLE " + article_sid + " END\n")
        # Do what the server told us to.
        # Command MUST be SLEEP. We will sleep for at least self.MIN_SLEEP
        command = response.read()
        self._handleCommand(command, do_sleep=True)


#TODO(macambira): move main out of this module or refactor it into a set of small helper functions


def main(base_url, store_dir):
    """Setup enviroment and run client."""
    global log

    hostname = gethostname()

    id_filename = hostname + '.id'
    log_filename = hostname + '.log'

    log = open(log_filename, 'a', 0)  # Redirecting log from output to file
    client_id = getUUID(id_filename)

    cli = SlashClient(client_id, base_url=base_url, store_dir=store_dir)
    sys.stderr.write("\nStarting Client...\n")
    log.write("STARTED " + time.asctime() + "\n")

    #print "\nbase_url: ", base_url,"\n"
    #print "store_dir: ", store_dir,"\n"
    #print "Cliente-id : ", cli.id,"\n"

    cli.run()


if __name__ == '__main__':
    
    #BASE_URL = 'http://localhost:8000'
    #STORE_DIR = os.getcwd()  # "." loses its meaning as soon as we deamonize
    
    BASE_URL = 'http://www.speed.dcc.ufmg.br/slashcrawling'
    STORE_DIR = '/home/ufmg_socnets/client/article_data/'

    # Dettach the current proceess from the terminal and became 
    # a daemon
    print "Becoming a daemon"
    res = createDaemon()
    # We closed all stdio and redirected them for /dev/null
    # Just in case we need them back, let's reconfigure stdout and stderr
    reconfigStdout(STORE_DIR + "/daemon.log")
    log = sys.stdout # we closed the old file descriptor, get the new one.
    print "Became a daemon"
    
    try:
        main(BASE_URL, STORE_DIR)
    except urllib2.HTTPError, e:
        log_urllib2_exception(e)
        raise
    except:
        log_backtrace()
        raise
 
# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :
