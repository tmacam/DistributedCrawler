#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Based on code in private repository:
#     upload_aux.py 243 2006-11-22 09:20:37 -0200 tmacam 
#

"""Basic infrastructure for DistributedCrawling clients.

A DistributedCrawling client periodically contact a DistributedCrawling server
in order to receive from it commands -- be it a request to "call me back in S
seconds from now" or "retrive page P and upload it to me".

It is the client's job avoing "getting caught" (entering in a black list)
crawling a site. Thus, the client MUST make sure it will not make too many
request for the site in a short timeframe. This is implemented in BaseClient by
defining minimum ammount of time (in seconds) that the client will sleep between
two consecutive non-sleep commands. This setting, BaseClient.MIN_SLEEP, is
enforced even if the server instructs it to do too much work in a short time
interval.
"""

__version__ = "0.3.posdigg"
__date__ = '2006-11-22 09:20:37 -0200 (Wed, 22 Nov 2006)'
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'


import sys
import os
import urllib2
import time
import traceback
from socket import gethostname, getfqdn 
from urllib import urlencode
import uuid
import upload_aux
from BeautifulSoup import BeautifulSoup
from digg_article_retriever import Article_Retriever, NothingForYouToSeeErrorPage, \
                             __version__ as articleretriever_version
from daemonize import createDaemon


# TODO(macambira): perhaps python now has a nice logging framework
log = sys.stdout


class WrongCommandFormat(Exception):
    """We received a malformed command from the server."""
    pass


class BaseClient(object):
    """Our client :-)

    The client periodically contacts the server, executes the "commands" it
    sends and sleeps. This processes is repeated endlessly.

    Commands are handled by "Command Handlers", methods implemented locally that
    perform what is necessary for completing the order informed in a command.
    Those methods are registered in self.handlers, a dictionary where keys are
    the command's ACTION name and the methods for carring such actions are the
    values. By default, the SLEPP command is already registered (BaseClient).
    Descendents should extend this dictionary with their own methods.

    Class Atributes
    ---------------

    MIN_SLEEP: Minimum ammount of time that client will speel between handling
        two commands. This overides server commands if needed.
    """

    MIN_SLEEP = 240

    def __init__(self, client_id, base_url, store_dir=None):
        """BaseClient constructor.

        Must be called in descendets.

        @param  client_id   The uniq identifier of this client, used by the
                            server to tell clients apart. IDs SHOULS be UUIDs
                            and it is recomended that they do not change accross
                            invocations in the same box/account. See getUUID.
        @param  base_url    Server's base URL. All DistributedCrawling server 
                            URLs are made using this as base path.
        @param  store_dir   If not None, defines a directory where items
                            retrived be the client will be locally saved.
                            The directory will be created if it does not exist.
        """
        # Setup Handlers
        self.handlers = {'SLEEP' : self.sleep}
        self.id = client_id
        self.base_url = base_url
        self.headers = {'client-id' : client_id,
                        'client-hostname' : getfqdn(),
                        'client-version' : __version__,
                        'client-arver': articleretriever_version}
        # Setup store
        self.store_dir = store_dir
        if self.store_dir:
            # Try to create this directory if it doesn't exist
            if not os.path.isdir(store_dir):
                os.makedirs(store_dir)

    def run(self):
        """Start event loop - endlessly contact the server for new commands."""
        # Common setup
        ping_url = self.base_url + '/ping'
        ping_req = urllib2.Request(ping_url, headers=self.headers)
        # Retry setup
        n_attempts = 0
        # Time to sleep in minutes
        sleep_delay = 0     
        while 1==1:
            try:
                try:
                    log.flush()
                    fh = urllib2.urlopen(ping_req)
                    command = fh.read()
                    self._handleCommand(command)
                    # Success! Reset retry counters
                    n_attempts = 0
                    sleep_delay = 0
                except urllib2.HTTPError, e:
                    log_urllib2_exception(e)
                    raise
                except:
                    log_backtrace()
                    raise
            except:
                if n_attempts > 5:
                    log.write("RUN - GIVING UP AFTER %i ATTEMPTS\n" % n_attempts)
                    raise
                sleep_delay = sleep_delay + 15
                n_attempts = n_attempts + 1
                log_msg_fmt = "RUN - IGNORING ERROR. I WILL RETRY IN %i MINS\n"
                log.write(log_msg_fmt % sleep_delay)
                time.sleep(sleep_delay * 60)

    def _handleCommand(self, command, do_sleep=False):
        """Parses and handles a given command.

        We also make sure that we don't do successive requests to the site in
        less than MIN_SLEEP here.

        Observe that after submitting the result of an job to the server we will
        receive another command. Althought it ought to be a SLEEP command,  in
        some weird cases the server can give us something else. In order to
        avoid ignoring the MIN_SLEEP setting, callers of this function should
        set do_sleep to True in "after-job-mission-command-handling". 

        @param command   A command (string) given by the server
        @param do_sleep  If set to True, we will ignore the command if it is
                         not SLEEP command. If it is, we will sleep for at least
                         self.MIN_SLEEP, no matter what the ammount given in the 
                         command is.
        """
        command = command.strip()
        action, param, trailer = command.split()
        if trailer != '#':
            raise WrongCommandFormat(command)
        if do_sleep:
            if command != 'SLEEP' or int(param) < self.MIN_SLEEP:
                self.handlers['SLEEP'](self.MIN_SLEEP)
            else:
                self.handlers['SLEEP'](param)
        else:
            self.handlers[action](param)

    def _write_to_store(self, article_id, data):
        """Write some sort of retrieved data (article) to the local store directory.

        This is only performed if self.store_dir was set during this instance's
        creation.

        @param article_id  identifier of article or data. Should be a
                           filesystem-friendly filename.
        @param data        A StringIO object with the article contents. It is
                           good practice to have data compressed in gzip format.
        """
        if self.store_dir:
            filename = os.path.join(self.store_dir, str(article_id))
            fh = open(filename, 'wb')
            fh.write(data.getvalue())

    # ACTIONS
    def sleep(self, param):
        """Handler for the SLEEP command.

        This handler should probably not be overwriten in descentets.
        """
        time_str_fmt = "%Y%m%dT%H%M%S"
        now = time.strftime(time_str_fmt)
        wake_up_time = time.strftime(time_str_fmt, \
                                     time.localtime(time.time() + int(param)))
        log.write("PING, sleeping for %s (now: %s wake: %s)\n" % \
                  (str(param), now, wake_up_time))
        time.sleep(float(param))


######################################################################
# Example Clients
#
# TODO(macambira): move'em out of this module
######################################################################


class PongClient(BaseClient):
    """Dummy client."""
    def __init__(self, client_id, base_url, store_dir=None):
        """DiggClient Constructor."""
        # Parent class constructor
        BaseClient.__init__(self, client_id, base_url, store_dir)
        # Registering Command Handlers
        self.handlers['PONG'] = self.pong

    def pong(self, params):
        """Command handler that just prints pong in the screen."""
        print "PONG!"
        # Ok. Command, handled. Now what?
        # Since we don't contact the server after handling this
        # command, we just return from here -- there is no
        # need nor reason to call _handleCommand(..., do_sleep=True)


class DiggClient(BaseClient):
    """Client for crawling digg articles."""
    def __init__(self, client_id, base_url, store_dir=None):
        """DiggClient Constructor."""
        # Parent class constructor
        BaseClient.__init__(self, client_id, base_url, store_dir)
        # Registering Command Handlers
        self.handlers['ARTICLE'] = self.article

    def _write_to_store(self, article_id, data):
            """Write a (compressed) article to store.
            
            Article ids is turned into something filesystem safe here.
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


class SlashClient(BaseClient):
    """Client for crawling Slashdot Articles."""
    def __init__(self, client_id, base_url, store_dir=None):
        """DiggClient Constructor."""
        # Parent class constructor
        BaseClient.__init__(self, client_id, base_url, store_dir)
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
        """Download a the list of articles published in a given day -- a issue."""
        issue_url = "http://slashdot.org/index.pl?issue=" + params
        # Get and parse the issue
        log.write("ISSUE " + issue_url + "\n")
        data = urllib2.urlopen(issue_url).read()
        soup = BeautifulSoup(data)
        urls = [i.a['href'] for i in soup.findAll(['li','span'],attrs={'class':['more','storytitle']})]
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
            response = upload_aux.upload_form(upload_url, form_data,\
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


######################################################################
# Auxiliary functions
#
# TODO(macambira): move main out of this module or refactor it into a set of small helper functions
######################################################################


def getUUID(filename):
    # Try opening the file for reading
    try:
        fh = open(filename, 'r')
        id = fh.read()
        if len(id) < 36:
            # not a valid ID
            id = create_and_write_id(filename)
    except IOError:
        # File (probably) doesn't exist. Generate and ID and add it to the file
        id = create_and_write_id(filename)
    return id.strip()

def create_and_write_id(filename):
    id = str(uuid.uuid1())
    fh = open(filename, 'w')
    fh.write(id)
    fh.close()
    return id


def log_backtrace():
    global log
    log.write("EXCEPTION in user code:\n")
    log.write('-'*60 + "\n")
    traceback.print_exc(file=log)
    log.write('-'*60 + "\n")
    log.flush()

def log_urllib2_exception(exp):
    global log
    log.write("EXCEPTION in user code:\n")
    log.write('-'*60 + "\n")
    traceback.print_exc(file=log)
    log.write("EXCEPTION server response:\n")
    log.write('-'*60 + "\n")
    log.write( "\n".join([ ":".join(k,v) for k,v in e.info().items()]) )
    log.write('-'*60 + "\n")
    log.write(str(e.read()))
    log.write('-'*60 + "\n")
    log.flush()

def reconfigStdout(stdout=None, stderr=None):
    """Reconfigures standard output file descriptor.

    This is functio should be called after createDaemon.

    @param stdout Path to a file we will open for output and setup for use
      as stdout. If None is supplied, we WON'T reconfigure stdout.
    @param stderr. Path to a file we will open of append and setup for use
      as stderr. If None is supplied but one was for stdout, we will bind the
      two file descriptors togueter. If None was supplied for both nothing will
      be done.
    """
    if stdout:
        sys.stdout.close()
        os.close(1)
        sys.stdout = open(stdout, 'w', 1) # redirect stdout

    if not (stderr is None and stdout is None):
        sys.stderr.close()
        os.close(2) # and associated fd's
        if stdout:
            os.dup2(1, 2) # fd 2 is now a duplicate of fd 1
            sys.stderr = os.fdopen(2, 'a', 0) # redirect stderr
        else:
            sys.stderr = open(stderr, 'a', 0)


def main(base_url, store_dir):
    global log

    hostname = gethostname()

    id_filename = store_dir + "/" + hostname + '.id'
    log_filename = store_dir + "/" + hostname + '.log'

    # Redirecting log from output to file
    log = open(log_filename,'a',0) 
    id = getUUID(id_filename)

    c = Client( id, base_url=base_url, store_dir=store_dir)
    sys.stderr.write("\nStarting Client...\n")
    log.write("STARTED " + time.asctime() + "\n")
    
    #print "\nbase_url: ", base_url,"\n"
    #print "store_dir: ", store_dir,"\n"
    #print "Cliente-id : ", c.id,"\n"

    c.run()


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
