#!/usr/bin/python
# -*- coding: utf-8 -*-

# Based on code in private repository:#
#   articleretriever.py 244 2006-11-23 15:20:50Z tmacam
#

"""Basic infrastructure and functions for retrieving and processing pages.

The main focus of DistributedCrawling is to collect articles and
their comments from news sites. The pages of those articles share
a very similar strucutre:
    + some of them have the article content paginated
    + most of them have the comments paginated

Aditionally, some of those sites are rather picky in returning
meaningfull content to "crawlers". For this reason, we try as much
as possible to make our requests look like a genuine browser requests,
though we use a distinctive User Agent.

It is also not uncommon for some of this sites returning erractic
pages from time to time, a fact that prompts for validating every
page received from them.

Most of the code in this module tries to deal with this issues,
providing auxiliary tools to dealing with the retrieaval of one full
article page from these sites.
"""

import time
import urllib
import urllib2
import gzip
import zlib
import re
import logging
from BeautifulSoup import BeautifulSoup


# Get the fastest StringIO implementation available
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO


__all__ = ['decompress_downloaded_page', 'AbstractArticleRetriever']
__version__ = "0.3.posdigg"
__date__ = '$Date: 2006-11-23 13:20:50 -0200 (Thu, 23 Nov 2006) $'
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006 Tiago Alves Macambira'
__license__ = 'X11'


def decompress_downloaded_page(page):
    """Gunzip or deflate a (possibily) compressed page.

    Based on code from urlutils.py - Simplified urllib handling
    Written by Chris Lawrence <lawrencc@debian.org>
    (C) 1999-2002 Chris Lawrence

    Args:
        page: a file-like object as returned by urllib2.urlopen
    """
    encoding = page.info().get("Content-Encoding")
    # note: some servers send content encoding gzip if file ends with ".gz"
    # but we don't want to decompress such files
    if encoding in ('gzip', 'x-gzip', 'deflate') and \
       not page.geturl().endswith(".gz"):
        # cannot seek in socket descriptors, so must get content now
        content = page.read()
        if encoding == 'deflate':
            fp = StringIO.StringIO(zlib.decompress(content))
        else:
            fp = gzip.GzipFile('', 'rb', 9, StringIO.StringIO(content))
        # remove content-encoding header
        headers = {}
        ceheader = re.compile(r"(?i)content-encoding")
        for h in page.info().keys():
            if not ceheader.match(h):
                headers[h] = page.info()[h]
        newpage = urllib.addinfourl(fp, headers, page.geturl())
        if hasattr(page, "code"):
            # python 2.4 compatibility
            newpage.code = page.code
        if hasattr(page, "msg"):
            # python 2.4 compatibility
            newpage.msg = page.msg
        page = newpage
    return page


class InvalidPage(Exception):
    """Exception used to signal that the contents of a downloaded page
    are invalid.

    See AbstractArticleRetriever.validate_page() and
    AbstractArticleRetriever.get_page_from_url() for more information on
    where such exception can be raised.
    """
    pass


class AbstractArticleRetriever(object):
    """Base class for construction of Article Retrievers.

    We just setup some headers here -- most of the code still is
    implementation/site dependent.

    Desdendents MUST overwrite the validate_page() and the
    get_article_soup() methods.

    Class attributes
    ----------------

    USER_AGENT: User-Agent informed to the crawled site by HTTP headers.

    COMMOM_HEADERS: Common HTTP headers that should be passed back to the
        crawled site server. Their purpose is to make us look more like a
        browser than like a crawler.
    """

    USER_AGENT = 'Papudim/0.90 BETA/' + __version__

    COMMOM_HEADERS = { 'User-Agent' : USER_AGENT,
            'Accept':'text/xml,application/xml,application/xhtml+xml,text/html',
            'Accept-Language' : 'en-us;q=0.7,en;q=0.3',
            'Accept-Encoding' : 'gzip;q=1.0, deflate;q=0.9, identity;q=0.5',
            'Accept-Charset' : 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            }

    def __init__(self, extra_headers=None):
        """AbstractArticleRetriever Constructor.

        Descendents should call this constructor just to setup common instance
        attributes.

        @param story_id Identifier of the article
        @param total_comments Total number of comments
        @param extra_headers extra headers sent on every HTTP request made by
                   this client
        """
        # Setup headers
        if not extra_headers:
            extra_headers = {}
        self.headers = dict(self.COMMOM_HEADERS)
        self.headers.update(extra_headers)

    def validate_page(self, page):
        """Verifies if the contents of a downloaded page are valid.
        
        MUST BE OVERWRITEN BY DESCENDENTS.

        @param page A BeautifulSoup object with the contents of the
                    downloaded page.

        @return True, if the page is valid. False othersise:
        
        @throw InvalidPage or some descendent of it.
        """
        raise NotImplementedError()
        
    def get_article_soup(self):
        """Return a BeautifulSoup instance of the whole article.

        MUST BE OVERWRITEN BY DESCENDENTS.
        
        @return the article as a BeautifulSoup object.
        """
        raise NotImplementedError()

    def get_page_from_url(self, url, n_retries=3, sleep_for=10):
        """Get the page (a BeautifulSoup instance) for a given URL.

        @param url The page's URL
        @param n_retries Maximum number of attempts done before givin up.
        @param sleep_for Ammount of time to wait before attempts

        It verifies page is a valid URL document. At most n_retries
        are done before giving up.

        If in the last attempt the page retrieved is not a valid page,
        as informed by validate_page(), then InvalidPage is raised.
        Network errors in the last attempt will not be masked and
        their correspondig exceptions will also be raised.

        @throws InvalidPage or any other exception raised by
            validate_page()

        @returns a BeautifulSoup object corresponding to the contents
            of the informed url. These contents were validated by
            validate_page().
        """
        req = urllib2.Request(url, headers=self.headers)
        for attempt in range(1 + n_retries):
            try:
                time.sleep(sleep_for)
                fh = decompress_downloaded_page(urllib2.urlopen(req))
                page = BeautifulSoup(fh.read())
                self.validate_page(page)
            except:
                logging.warning('AbstractArticleRetriever %s RETRY #%i\n' % \
                        (url, attempt))
                if attempt == n_retries:
                    # Was that the last retry? Give it up, dude, it's doomed
                    raise
                else:
                    # Exponetial backoff
                    sleep_for = sleep_for + 60
                    continue
        return page

    def get_article(self):
        """Return the whole article, with all comments page merged.

        @return The article contents, as a string.
        """
        return str(self.get_article_soup())

    def get_article_compressed(self):
        """Returns a StringIO with the article contents compressed with gzip."""

        mem_file = StringIO.StringIO()
        gzip_file = gzip.GzipFile(mode='wb', fileobj=mem_file)
        gzip_file.write(self.get_article())
        gzip_file.close()
        mem_file.seek(0)
        return mem_file

           
# vim: set ai tw=80 et sw=4 sts=4 ts=4 fileencoding=utf-8 :
