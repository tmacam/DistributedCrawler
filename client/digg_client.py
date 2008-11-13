#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Based on code in private repository:
#     client.py 243 2006-11-22 09:20:37 -0200 tmacam 
#

"""DistributedCrawling client for Digg."""

__version__ = "0.3.posdigg"
__date__ = '2006-11-22 09:20:37 -0200 (Wed, 22 Nov 2006)'
__author__ = "Tiago Alves Macambira & Claudiane Fonseca"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'


import sys
import os
import urllib2
import time
from socket import gethostname
import upload_aux
from digg_article_retriever import Article_Retriever, \
        __version__ as articleretriever_version
from daemonize import createDaemon, reconfigStdout
from client import BaseClient, getUUID, log_backtrace, log_urllib2_exception


# TODO(macambira): perhaps python has a nice logging framework now
log = sys.stdout


class DiggClient(BaseClient):
    """Client for crawling digg articles."""
    def __init__(self, client_id, base_url, store_dir=None):
        """DiggClient Constructor."""
        # Parent class constructor
        BaseClient.__init__(self, client_id, base_url, store_dir)
        # Informing the version of our ArticleRetriver
        self.headers["client-arver'"] =  articleretriever_version
        # Registering Command Handlers
        self.handlers['ARTICLE'] = self.article

    def _write_to_store(self, article_id, data):
        """Write a (compressed) article to store.
        
        Article_id is turned into something filesystem safe here.
        """
        safe_id = article_id.replace('/', '_')
        safe_id += '.xml.gz'
        BaseClient._write_to_store(self, safe_id, data)

    def article(self, params):
        """Retrieve an article and send it to the server."""
        # Retrieve the article's id and its number of comments
        server_data = params.replace('/', ' ')
        server_data = server_data.strip()
        story_id, total_comments = server_data.split()
        # Download article
        log.write( "ARTICLE " + str(story_id) + " BEGIN\n")
        downloader = Article_Retriever(story_id, total_comments)
        compressed_article = downloader.get_article_compressed()
        self._write_to_store(story_id, compressed_article)
        log.write( "ARTICLE " + str(story_id) + " GOT COMPRESSED DATA\n")
        # Setup upload form and headers
        upload_headers = dict(self.headers)
        form_data = {'article-data' : compressed_article,
                     'article-sid'  : params,
                     'client-id'    : self.id}
        # Upload the article
        upload_url = self.base_url + '/article/' + params
        response = upload_aux.upload_form(upload_url, form_data, upload_headers)
        log.write( "ARTICLE " + story_id + " END\n")
        # Ok. Command, handled. Now what?
        # Do what the server told us to.
        # Command MUST be SLEEP. We will sleep for at least self.MIN_SLEEP
        command = response.read()
        self._handleCommand(command, do_sleep=True)


#TODO(macambira): move main out of this module or refactor it into a set of small helper functions


def main(base_url, store_dir):
    """Setup enviroment and run client."""
    global log

    hostname = gethostname()

    id_filename = store_dir + "/" + hostname + '.id'
    log_filename = store_dir + "/" + hostname + '.log'

    log = open(log_filename, 'a', 0)  # Redirecting log from output to file
    client_id = getUUID(id_filename)

    cli = DiggClient(client_id, base_url=base_url, store_dir=store_dir)
    sys.stderr.write("\nStarting Client...\n")
    log.write("STARTED " + time.asctime() + "\n")

    #print "\nbase_url: ", base_url,"\n"
    #print "store_dir: ", store_dir,"\n"
    #print "Cliente-id : ", cli.id,"\n"

    cli.run()


if __name__ == '__main__':
    BASE_URL = 'http://www.speed.dcc.ufmg.br/digg'
    STORE_DIR = os.getcwd()  # "." loses its meaning as soon as we deamonize

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
