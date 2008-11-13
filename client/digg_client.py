#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = "$Revision: 243 $".split()[1]

import sys
import os
import urllib2
import time
import traceback
from socket import gethostname, getfqdn; 
from urllib import urlencode
import uuid
import upload_aux
from BeautifulSoup import BeautifulSoup
from digg_article_retriever import Article_Retriever, NothingForYouToSeeErrorPage, __version__ as articleretriever_version
from daemonize import createDaemon

log = sys.stdout

class WrongCommandFormat(Exception): pass

class Client(object):
    """Our client :-)

    The client periodically contacts the server, executes the "commands" it
    sends and sleeps. This processes is repeated endlessly.
    """

    MIN_SLEEP = 240

    def __init__(self,id, base_url, store_dir=None):
        """
        @param  id          The id of this client, used by the serveer to tell
                            clients apart.
        @param  base_url    Server's base URL. All URLs are made using this as
                            base path.
        @param  store_dir   If not None, defines the place where article
                            retrived will be locally saved. The directory will
                            be created if it does not exist.
        """
        # Setup Handlers
        self.handlers = { 'SLEEP': self.sleep, 'ARTICLE': self.article,}
        self.id = id
        self.base_url = base_url
        self.headers = {'client-id': id,
                        'client-hostname': getfqdn(),
                        'client-version' : __version__,
                        'client-arver': articleretriever_version}

        self.store_dir = store_dir
        if self.store_dir:
            # Try to create this directory if it doesn't exist
            if not os.path.isdir(store_dir):
                os.makedirs(store_dir)


    def run(self):
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
                    log.write("RUN - GIVING UP AFTER %i ATTEMPTS\n" %n_attempts)
                    raise
                sleep_delay = sleep_delay + 15
                n_attempts = n_attempts + 1
                log.write("RUN - IGNORING ERROR. I WILL RETRY IN %i MINS\n" %sleep_delay)
                time.sleep(sleep_delay * 60)



    def _handleCommand(self, command, do_sleep=False):
        """Parses and handles a given command

        @param command:  A command (string) given by the server
        @param do_sleep: If set to True, we will ignore the command if it is
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
                self.handlers[action](param)
        else:
            self.handlers[action](param)        



    def _write_to_store(self, article_id, data):
        """Write an artitle to the local store directory.

        This is only performed if self.store_dir was set during this instance
        creation.

        @param article_id: identifier of article
        @param data:       A StringIO object with the article contents compressed
                           in gzip format.
        """
        if self.store_dir:
            #escaped_id = article_id.replace('/','_')
            filename = os.path.join(self.store_dir, str(article_id)  + '.xml.gz')
            fh = open(filename,'wb')
            fh.write(data.getvalue())


    def sleep(self, param):

        now = time.strftime("%Y%m%dT%H%M%S")
        wake_up_time = time.strftime( "%Y%m%dT%H%M%S", time.localtime(time.time() + int(param)) )
        log.write( "PING, sleeping for %s (now: %s wake: %s)\n" % ( str(param),now, wake_up_time))
        #print "PING, sleeping for:",str(param), "now: ", now, "wake: ", wake_up_time, "\n" 

        time.sleep(float(param))
        

    def article(self, params):
        
        # Get the article
        server_data = params.replace('/',' ')
        server_data = server_data.strip()
        story_id, total_comments = server_data.split()

        #print "\narticle_id: ", story_id
        #print "total_comments: ", total_comments

        log.write( "ARTICLE " + str(story_id) + " BEGIN\n")
        
        downloader = Article_Retriever(story_id, total_comments)

        compressed_article = downloader.get_article_compressed()

        self._write_to_store(story_id,compressed_article)
        log.write( "ARTICLE " + str(story_id) + " GOT COMPRESSED DATA\n")
        
        # Setup upload form and headers
        upload_headers = dict(self.headers)
        form_data = {'article-data' : compressed_article, 'article-sid'  : params, 'client-id': self.id}
        #print "Form-data: ", form_data, "\n"

        # Upload the article
        upload_url = self.base_url + '/article/' + params
        #print "upload_url: ", upload_url, "\n"

        response = upload_aux.upload_form(upload_url, form_data, upload_headers)

        log.write( "ARTICLE " + story_id + " END\n")
        
        # Do what the server told us to.
        # Command MUST be SLEEP. We will sleep for at least self.MIN_SLEEP
        command = response.read()
        #print command
        self._handleCommand(command, do_sleep=True)
        
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


def main(base_url,store_dir):
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
