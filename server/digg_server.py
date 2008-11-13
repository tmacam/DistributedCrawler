#!/usr/bin/python
# -*- coding: utf-8 -*-

"""A Distributed Crawler for digg articles."""

__version__ = "0.3.posdigg"
__date__ = "2008-09-29 21:09:46 -0300 (Mon, 29 Sep 2008)"
__author__ = "fonseca at ddc ufmg br & tmacam"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'

import os

from server import BaseControler, BaseDistributedCrawlingServer
from twisted.python import log
from twisted.python.logfile import DailyLogFile

class ArticleControler(BaseControler):
    """Task Controller tha receives retrieved Digg articles."""
    ACTION_NAME = "ARTICLE"
    PREFIX_BASE = "articles"

    def __init__(self, sched, prefix, client_reg, store_dir):
        """
        @param store_dir where the articles (compressed) will be stored.
        """
        BaseControler.__init__(self, sched, prefix, client_reg)
        # Setup a directory where we store received articles.
        # Try to create this directory if it doesn't exist
        if not os.path.isdir(store_dir):
            os.makedirs(store_dir)
        self.store_dir = store_dir

    def render_POST(self, request):
        """Process the article returned by a client."""
        client_id = self.client_reg.updateClientStats(request)
        # get the articleId
        article_sid = request.args['article-sid'][0]
        article_data = request.args['article-data'][0]
        # save the contents of the article
        escaped_sid = article_sid.replace('/', '_')
        fh_filename = os.path.join(self.store_dir, escaped_sid + '.xml.gz')
        fh = open(fh_filename, 'wb')
        fh.write(article_data)
        fh.close()
        # Ok! Article saved!
        self.markJobAsDone(article_sid)
        log.msg("ARTICLE %s done by client %s." % (article_sid, client_id))
        return self.scheduler.renderPing(client_id, just_ping=True)


def main():
    print "\nIniciando server...\n"

    PORT = 8700
    PREFIX = './db/'
    ARTICLE_STORE_DIR = './article_archive/'
    INTERVAL = 60

    # Setup logging
    logfile = DailyLogFile('diggcrawler.log', '.')
    log.startLogging(logfile)

    server = BaseDistributedCrawlingServer(PORT, PREFIX, INTERVAL)
    article_controler = ArticleControler(server.getScheduler(),
                                         PREFIX,
                                         server.getClientRegistry(),
                                        ARTICLE_STORE_DIR)
    server.registerTaskController(article_controler, 'article', 'Articles')
    server.run()
    
    

if __name__ == '__main__':
    main()

# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :
