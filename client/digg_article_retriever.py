#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Return articles from digg with all their comments in a single page."""

import sys
import os
import time
import urllib
import urllib2
import gzip
import zlib
import re
from BeautifulSoup import BeautifulSoup

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

__all__ = ['Article_Retriever' ]
__version__ = "$Revision: 244 $".split()[1]

class NothingForYouToSeeErrorPage(Exception):
    """An placeholder page for content that should exist but just vanished."""

class InvalidDiggCommentPage(Exception):
    """This i not a valid Digg comment page."""

class Article_Retriever(object):
    """Retrieves a full article page from Digg.

    It will download an article page with it's comments (flat mode, no
    threading, oldest first). If an article page was broken into multiple
    comments page it will retrieve all of them and merge them into a single
    page.

    """

    USER_AGENT = 'Papudim/0.90 BETA/' + __version__

    COMMOM_HEADERS = { 'User-Agent' : USER_AGENT,
            'Accept':'text/xml,application/xml,application/xhtml+xml,text/html',
            'Accept-Language' : 'en-us;q=0.7,en;q=0.3',
            'Accept-Encoding' : 'gzip;q=1.0, deflate;q=0.9, identity;q=0.5',
            'Accept-Charset' : 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            }

    def __init__(self, story_id, total_comments, log=sys.stderr, extra_headers={}):
        """
        @param story_id Identifier of the article
	@param total_comments Total number of comments
        @param log A file-like object where DEBUG information will be written
                   during the processing of an article.
        @param extra_headers extra headers sent on every HTTP request made by
                   this client
        """
       
	self.URL_PREFIX = "http://services.digg.com/stories/"
    	self.URL_SUFIX = "&appkey=http%3A%2F%2Fexample.com&type=xml"
	self.url_basic = self.URL_PREFIX + str(story_id)
 
	self.story_id = story_id
	self.total_comments = total_comments
    	
	# Setup logging
        self.log = log
        # Setup headers
        self.headers = dict(self.COMMOM_HEADERS)
        self.headers.update(extra_headers)
           
    def _get_next_pages_urls(self):
        """Get the URL's of the remaining pages of comments."""
	
	more_pages_urls = []

	numcomms = int(self.total_comments)
        rest = numcomms
        count = 100
        offset = 0

        while(rest > 100):
		link = self.url_basic + ",0/comments?offset=" + str(offset) + "&count=" + str(count) + self.URL_SUFIX 
		more_pages_urls.append(link)	
        	rest = rest - 100
            	offset = offset + count
	
	if rest != 0:	
        	last_link = self.url_basic + ",0/comments?offset=" + str(offset) + "&count="+ str(rest) + self.URL_SUFIX
		more_pages_urls.append(last_link)

	return more_pages_urls

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
        """
	req = urllib2.Request(url, headers=self.headers)
        for attempt in range(1 + n_retries):
            try:
                time.sleep(sleep_for)
                fh = self._decode(urllib2.urlopen(req))
                page = BeautifulSoup(fh.read())
		if not page.find('events'):
		    raise InvalidDiggCommentPage(url, fh.info(), page)
            except:
		self.log.write('ARTICLE WARN ArticleRetriever %s RETRY #%i\n' %(url, attempt) )
                if attempt == n_retries:
                    # Was that the last retry? Give it up, dude, it's doomed
                    raise
                else:
                    # Exponetial backoff
                    sleep_for = sleep_for + 60
                    continue
        return page

    def _merge_comments(self, event_root, page_data):
	for comment in page_data.find('events'):
	    event_root.append(comment)
	

    def _merge(self, more_pages, sleep_for=10):
	"""Merges the comments in a page with the comments we already have"""
	# Download everything
    	page_data = []
    	for url in more_pages:
        	page_data.append(self._get_page_from_url(url))
    	# Merge them
    	page_root = page_data[0]
    	event_root = page_root.find('events')
    	for data in page_data[1:]:
        	self._merge_comments(event_root, data)
       
    	#print len(event_root.findAll('comment'))
    	return event_root
	
    def _get_article_soup(self):
        """Return a BeautifulSoup instance of the whole article."""
        
	whole_article = []
	# Get the whole article
	if self.total_comments <= 100:
		url = self.url_basic + ",0/comments?count=" + str(self.total_comments) + self.URL_SUFIX
		print url
		whole_article = self._get_page_from_url(url)
        
	# Merge all pages of article 
	else:
		more_pages = self._get_next_pages_urls()
		#print "\n---All pages---:", more_pages, "\n"	
		whole_article = self._merge(more_pages)

	#print whole_article
        return whole_article

    def get_article(self):
        """Return the whole article, with all comments page merged."""
        
	return str(self._get_article_soup())

    def get_article_compressed(self):
        """Returns a StringIO with the article contents compressed with gzip."""

        mem_file = StringIO.StringIO()
        gzip_file = gzip.GzipFile(mode='wb',fileobj=mem_file)
        gzip_file.write(self.get_article())
        gzip_file.close()
        mem_file.seek(0)
        return mem_file

           
if __name__ == '__main__':
    
    story_id = 1105010
    total_comments = 423

    store_dir = '/tmp/article_data/'

    article_ret = Article_Retriever(story_id, total_comments)
    
    #article = article_ret.get_article()

    gziped_article = article_ret.get_article_compressed()
   
    filename = os.path.join(store_dir, str(story_id)  + '.xml.gz')
    fh = open(filename,'wb')
    fh.write(gziped_article.getvalue())

    
# vim: set ai tw=80 et sw=4 sts=4 ts=4 fileencoding=utf-8 :
