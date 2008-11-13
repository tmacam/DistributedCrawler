#!/usr/bin/python
# -*- coding: utf-8 -*-

# Based on code in private repository:
#   articleretriever.py 244 2006-11-23 15:20:50Z tmacam 
#


"""Retrives articles from slashdot with all their comments in a single page.

DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED
DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED

This module contains legacy code. Please keep this in mind while reading it.
You should read article_retriever.py for a more up-to-date code.

DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED
DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED DEPRECATED
"""

import sys
import os
import time
import urllib
import urllib2
import gzip
import zlib
import re
from BeautifulSoup import BeautifulSoup


# Get the fastest StringIO implementation available
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO


__all__ = ['ArticleRetriever', 'LocalArticleRetriever', 'NoComments',
           'InvalidSlashdotCommentPage', 'NothingForYouToSeeErrorPage' ]
__version__ = "0.3.posdigg"
__date__ = '$Date: 2006-11-23 13:20:50 -0200 (Thu, 23 Nov 2006) $'
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006 Tiago Alves Macambira'
__license__ = 'X11'


class NoComments(Exception):
    """The page has no comments."""


class InvalidSlashdotCommentPage(Exception):
    """This i not a valid Slashdot comment page."""


class NothingForYouToSeeErrorPage(Exception):
    """An placeholder page for content that should exist but just vanished."""


class ArticleRetriever(object):
    """Retrieves a full article page from Slashdot.

    It will download an article page with it's comments (flat mode, no
    threading, oldest first). If an article page was broken into multiple
    comments page it will retrieve all of them and merge them into a single
    page.

    Instance Variables
    ------------------

      - self.first_page: a BeautifulSoup instance with the first page of the
        article.

      - self.current_comments: The list of comments we have so far. It is a
        BeautifulSoup Tag corresponding to first_page's <ul id='commentlisting'>
        tag.
    """

    ARTICLE_URL_TEMPLATE = 'http://slashdot.org/article.pl?sid=%s&threshold=-1&mode=flat&commentsort=4'

    USER_AGENT = 'Papudim/0.90 BETA/' + __version__

    COMMOM_HEADERS = { 'User-Agent' : USER_AGENT,
            'Accept':'text/xml,application/xml,application/xhtml+xml,text/html',
            'Accept-Language' : 'en-us;q=0.7,en;q=0.3',
            'Accept-Encoding' : 'gzip;q=1.0, deflate;q=0.9, identity;q=0.5',
            'Accept-Charset' : 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            }

    def __init__(self, sid, url=None, log=sys.stderr, extra_headers={}):
        """
        @param sid the sid of the article one wants to retrieve. Can be set to
                   None to force the download of a given URL as article.
        @param url If is is None, url is used instead as the path of the first
                   page of the article
        @param log A file-like object where DEBUG information will be written
                   during the processing of an article.
        @param extra_headers extra headers sent on every HTTP request made by
                    this client
        """
        if sid is None:
            self.sid = 'NO-ARTICLE-SID'
            self.first_page_url = url
            referer = url
        else:
            self.sid = sid
            self.first_page_url = self.ARTICLE_URL_TEMPLATE % sid
            referer = "http://slashdot.org/index.pl?issue=20" + \
                        "".join( sid.split("/")[0:3] )
        # Setup logging
        self.log = log
        # Setup headers
        self.headers = dict(self.COMMOM_HEADERS)
        self.headers.update(extra_headers)
        self.headers['Referer'] = referer

    def _get_next_comment_pages_urls(self):
        """Get the URL's of the remaining pages of comments."""
        links_div = self.first_page.find('div', 'linkCommentPage')
        if links_div:
            links_to_other_pages = links_div.findAll('a')
            return [ 'http:' + link['href'] for link in links_to_other_pages ]
        else:
            # Ops! No link to other pages, this is it...
            return []

    def _decode(self, page):
        """
        Gunzip or deflate a compressed page.

        Based on code from urlutils.py - Simplified urllib handling
        Written by Chris Lawrence <lawrencc@debian.org>
        (C) 1999-2002 Chris Lawrence
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

    def _get_page_from_url(self, url, n_retries=3, sleep_for=10):
        """Get the page (a BeautifulSoup instance) for a given URL.

        @param url The page's URL
        @param n_retries Maximum number of attempts done before givin up.
        @param sleep_for Ammount of time to wait before attempts

        It verifies page is a valid URL document. At most n_retries are done
        before raising giving up.

        If in the last attempt the page retrieved is not a valid Slashdot
        comments page then InvalidSlashdotCommentPage is raised. Network errors
        in the last attempt will not be masked and their correspondig exceptions
        will also be raised.
        """
        req = urllib2.Request(url, headers=self.headers)
        for attempt in range(1 + n_retries):
            try:
                # We are such a gentlemen
                time.sleep(sleep_for)
                # try to retrieve the page
                fh = self._decode(urllib2.urlopen(req))
                page = BeautifulSoup(fh.read())
                # Check to see if it is a valid Slashdot article/comments page
                # div[@id='comments'] together with div[@id='articles'] would
                # also be good filters for valid pages
                # For now, we just look for 
                # <div class="commentBoxForm" id="commentControlBox">
                # NOTICE: the id attribute may be missing in this tag...
                if not page.find('div', attrs={'id':'commentControlBox'}) and\
                   not page.find('div', attrs={'class':'commentBoxForm'}):
                    # Check against "Nothing for you to see here..." pages
                    NOTHING_ERR = u'Nothing for you to see here.'
                    article = page.find(attrs={'id':'article'})
                    if article and article.contents and article.contents[0] and\
                       article.contents[0].startswith(NOTHING_ERR):
                        raise NothingForYouToSeeErrorPage(url, fh.info(), page)
                    # Hum... just a regular invalid page
                    raise InvalidSlashdotCommentPage(url, fh.info(), page)

                # Success!
                break
            except NothingForYouToSeeErrorPage:
                # Retring will not sort things out: this page is doomed.
                # End of story. Give up.
                raise
            except:
                self.log.write('ARTICLE WARN ArticleRetriever %s RETRY #%i\n' %\
                                (url, attempt) )
                if attempt == n_retries:
                    # Was that the last retry? Give it up, dude, it's doomed
                    raise
                else:
                    # Exponetial backoff
                    sleep_for = sleep_for + 60
                    continue
        return page

    def merge(self, url):
        """Merges the comments in a page with the comments we already have.
        
        @param url  A comments-page url. It's comments will be extracted and
                        merged into our current list of comments
        """
        page = self._get_page_from_url(url)
        more_comments = page.find('ul',attrs={'id':'commentlisting'})
        if not more_comments:
            raise NoComments(url,page)
        for comment in more_comments:
            self.current_comments.append(comment)

    def _getArticleSoup(self):
        """Return a BeautifulSoup instance of the whole article."""
        # Get the first page of the article
        self.first_page = self._get_page_from_url(self.first_page_url)
        self.current_comments = self.first_page.find( 'ul', \
                                                  attrs={'id':'commentlisting'})
        if not self.current_comments:
            # First page can have no comments - that is not really an error
            # if the article does not have any comments.
            # <option value="-1" selected>-1: 0 comments</option>
            form = self.first_page.find('div', attrs={'id':'commentControlBox'})
            if form.select.option.string.split()[1] != '0':
                raise NoComments(self.first_page_url, self.first_page)
        comment_pages = self._get_next_comment_pages_urls()
        self.headers['Referer'] = self.first_page_url
        # Merge first page comments with the comments in other pages
        for next_page in comment_pages:
            self.log.write('ARTICLE %s ArticleRetriever %s\n' % (self.sid,\
                                                                 next_page,) )
            try:
                self.merge(next_page)
                self.headers['Referer'] = next_page
            except NoComments:
                # The last page may have no comments, but not the others
                if next_page != comment_pages[-1]:
                    raise
        self.log.write('ARTICLE %s ArticleRetriever ALL MERGED\n' % self.sid )

        return self.first_page

    def getArticle(self):
        """Return the whole article, with all comments page merged."""
        return str(self._getArticleSoup())

    def getArticleCompressed(self):
        """Returns a StringIO with the article contents compressed with gzip."""
        mem_file = StringIO.StringIO()
        gzip_file = gzip.GzipFile(mode='wb',fileobj=mem_file)
        gzip_file.write(self.getArticle())
        gzip_file.close()
        # Ok. Zipped file is in memory, but mem_file.read() won't return
        # anything unless we do a...
        mem_file.seek(0)
        return mem_file
        

class LocalArticleRetriever(ArticleRetriever):
    def __init__(self,files):
        cwd = os.getcwd() + '/'
        pages = []
        for f in files:
            if f.startswith('/'):
                # full path
                pages.append('file://' + f)
            else:
                # relative path
                pages.append('file://' + cwd + f)
        self.other_pages = pages[1:]

        ArticleRetriever.__init__(self,None,pages[0])
        
    def _get_next_comment_pages_urls(self):
        return self.other_pages
        

if __name__ == '__main__':
    if len(sys.argv[1:]) < 1:
        sys.stderr.write("Wrong number of arguments\n")
        sys.exit(1)
    first_page_sid = sys.argv[1]

    if first_page_sid.startswith('--local'):
        article_ret = LocalArticleRetriever(sys.argv[2:])
    else:
        article_ret = ArticleRetriever(first_page_sid)

    article = article_ret.getArticle()
    print article


# vim: set ai tw=80 et sw=4 sts=4 ts=4 fileencoding=utf-8 :
