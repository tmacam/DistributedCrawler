#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Simple (background) backtrace reporter."""


__version__ = "0.4.lastfm"
__date__ = '2008-12-11 17:45:37 -0200 (Thu, 11 Dec 2008)'
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'


import threading
import logging
import traceback
from DistributedCrawler.client import upload_aux
from StringIO import StringIO

class BacktraceReporter(threading.Thread):
    """Submits backtraces to the server in the background.
    
    The stacktrace will be analysed during instance construction in order
    to collect backtrace information to be sent to the server.
    """
    
    LOG = logging.getLogger("BacktraceReporter")

    def __init__(self, submission_url, headers, extra_params):
        """Constructor.
        Args:
            submission_url: URL to where backtraces reports should be submmited.
            headers: Headers to send toguether with the POST to the server.
            extra_params: dictionary with other information that should be
                sent togueter with the backtrace to the server. ClientID,
                client version and similar information should be sent here.
        """
        threading.Thread.__init__(self)  # Missing py3k super?!
        self.submission_url = submission_url
        self.headers = headers
        self.extra_params = extra_params
        # Get backtrace information from stack
        backtrace_log = StringIO()
        traceback.print_exc(file=backtrace_log)
        self.backtrace = backtrace_log.getvalue()

    def run(self):
        """Thread execution code...."""
        # Setup form and headers
        #    Although we used "upload" code, this is a plain POST
        try:
            upload_headers = dict(self.headers)
            form_data = {'backtrace': self.backtrace}
            form_data.update(self.extra_params)
            # Upload the article
            response = upload_aux.upload_form(self.submission_url, form_data,
                    upload_headers)
        except:
            # Really, we cannot do anything here.  We cannot report backtraces
            # on a a backtrace reporter, for god's sake
            self.LOG.exception("error while submitting backtrace.")
            pass


if __name__ == '__main__':
    import time
    logging.basicConfig(level=logging.DEBUG, flushlevel=logging.NOTSET)

    try:
        raise TypeError("Duh!!!")
    except TypeError:
        url = "http://localhost:8700/backtrace"
        sub = BacktraceReporter(url, {'client-id': 'blah'}, {'command':"COM"})
        sub.start()

    print "Done"

# vim: set ai tw=80 et sw=4 sts=4 ts=4 fileencoding=utf-8 :
