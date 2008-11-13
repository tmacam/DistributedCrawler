#!/usr/bin/python
# -*- coding: utf-8 -*-

# Based on code in private repository:
#     upload_aux.py 217 2006-11-08 23:08:24Z tmacam 
#

"""Auxiliary functions to deal with file uploading."""

import sys
import urllib2

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


__all__ = ['encode_multipart_formdata', 'split_form_fields_and_files',
           'upload_form']
__version__ = "0.3.posdigg"
__date__ = '2006-11-08 21:08:24 -0200 (Wed, 08 Nov 2006)'
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'


def encode_multipart_formdata(fields, files):
    """Encodes form data into a multipart/form-data string.

    @param fields   is a sequence of (name, value) elements for regular
                    form fields.
    @param files    is a sequence of (name, filename, value) elements for
                    data to be uploaded as files
    @return Return (content_type, body) ready for httplib.HTTP instance

    @see http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/146306
    """
    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    for (key, filename, value) in files:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
        L.append('Content-Type: application/octet-stream')
        L.append('')
        L.append(value.read())
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body


def split_form_fields_and_files(form_input):
    """Separate simple form fields from files-like objects

    This is an auxiliary function to use with encode_multipart_formdata. It's
    intent is to make using encode_multipart_formdata easier and to enhance code
    legibility.
    
    @return a tuple (fiels,files), in a format suitable for use with
    encode_multipart_formdata
    """
    fields = []
    files = []
    unknown_filename = 'unknown_filename'
    for key, val in form_input.items():
        if hasattr(val, 'read'):
            if hasattr(val, 'name'):
                filename = val.name
            else:
                filename = unknown_filename
            files.append((key, filename, val))
        else:
            fields.append((key, val))

    return (fields, files)


def upload_form(to, form_input=None, headers=None):
    """Upload send a form to a server
    
    @param to           The URL the POST request will be sent to.
    @param form_input   A dictionary-like structure with form data. Items may be
                        simple fields text fields and file-like objects.
    @param headers      Extra headers that must be sent with the request

    @return file-like object with the server response.
    """
    if form_input is None:
        form_input = {}
    if headers is None:
        headers = {}
    fields, files = split_form_fields_and_files(form_input)
    # Setup request
    content_type, body  =  encode_multipart_formdata(fields, files)
    sent_headers = {'Content-Type': content_type, \
                    'Content-Length': str(len(body))}
    sent_headers.update(headers)
    req = urllib2.Request(to, body, sent_headers)
    # Send  request and return response
    response = urllib2.urlopen(req)
    return response


######################################################################
# TODO(macambira): Example Code - should be moved out of this file.
######################################################################


def slashdot_main():
    """How to upload data to slashdot distributed crawler."""
    from articleretriever import ArticleRetriever, LocalArticleRetriever

    if len(sys.argv[1:]) < 1:
        sys.stderr.write("Wrong number of arguments\n")
        sys.exit(1)
    first_page_sid = sys.argv[1]

    if first_page_sid.startswith('--local'):
        first_page_sid = '2006/10/11/123123'
        article_ret = LocalArticleRetriever(sys.argv[2:])
    else:
        article_ret = ArticleRetriever(first_page_sid)

    # FIXME
    gziped_article = article_ret.getArticleCompressed()
    #gziped_article=open('/tmp/data.gz','rb')

    form_data = {'article-data' : gziped_article,
                 'article-sid'  : first_page_sid}
    my_headers = {'client-id' : 'xxxxxxxxxxxxxxxxxxxx'}
    #my_files = [('article-data','filename',gziped_article)]
    response = upload_form('http://localhost:8000/utest', form_data, my_headers)

    print response.read()


def digg_main():
    """How to upload data to slashdot distributed crawler."""
    from digg_article_retriever import Article_Retriever

    story_id = 190108
    total_comments = 214
    article_ret = Article_Retriever(story_id, total_comments)

    gziped_article = article_ret.get_article_compressed()

    form_data = {'article-data': gziped_article, 'article-sid': str(story_id)}

    my_headers = {'client-id' : 'xxxxxxxxxxxxxxxxxxxx'}
    
    response = upload_form('http://localhost:8700/utest', form_data, my_headers)

    print response.read()


if __name__ == '__main__':
    digg_main()


# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :
