#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Based on code in private repository:
#     client.py 243 2006-11-22 09:20:37 -0200 tmacam 
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


import os
import urllib2
import time
#import traceback
import socket
import uuid
import logging


# TODO create a logging hierarchy


######################################################################
# Base Classes
######################################################################


class WrongCommandFormat(Exception):
    """We received a malformed command from the server."""
    pass


class BaseClient(object):
    """Our client :-)

    The client periodically contacts the server, executes the "commands" it
    sends and sleeps. This processes is repeated endlessly.

    Commands are handled by "Command Handlers", methods implemented locally that
    perform what is necessary for completing an order informed in a command.
    Those methods are registered in self.handlers, a dictionary where keys are
    the command's ACTION name while the methods for carring such actions are the
    values. By default, the SLEPP command is already registered (BaseClient).
    Descendents should extend this dictionary with their own methods.

    Class Atributes
    ---------------

    MIN_SLEEP: Minimum ammount of time (in seconds) that client will sleep
        between handling two commands. This overides server commands if needed.
    """

    MIN_SLEEP = 240

    def __init__(self, client_id, base_url, store_dir=None):
        """BaseClient constructor.

        Must be called in descendents. Descendents should overwrite
        self.headers["client-aver"] with the version of the ArticleRetriever
        they use.

        @param  client_id   The unique identifier of this client, used by the
                            server to tell clients apart. IDs SHOULS be UUIDs
                            and it is recommended that they do not change accross
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
                        'client-hostname' : socket.getfqdn(),
                        'client-version' : __version__,
                        'client-arver' : "unknown"}
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
        while True:
            try:
                try:
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
                    logging.warning("RUN - GIVING UP AFTER %i ATTEMPTS",
                                    n_attempts)
                    raise
                sleep_delay = sleep_delay + 15
                n_attempts = n_attempts + 1
                logging.info("RUN - IGNORING ERROR. I WILL RETRY IN %i MINS",
                             sleep_delay)
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
            self.handlers['SLEEP'](max(self.MIN_SLEEP, int(param)))
        else:
            self.handlers[action](param)

    def _write_to_store(self, article_id, data):
        """Write some sort of retrieved data (article) into a file in the
        local store.

        This is only performed if self.store_dir was set during this instance's
        creation.

        @param article_id  identifier of article or data. Should be a
                           filesystem-friendly filename. Will be used as a
                           filename.
        @param data        A StringIO object with the article contents. It is
                           good practice to have data compressed in gzip format.
        """
        if self.store_dir:
            filename = os.path.join(self.store_dir, str(article_id))
            fh = open(filename, 'wb')
            fh.write(data.getvalue())
            logging.info("Wrote some data to local store: '%s'", filename)

    # ACTIONS
    def sleep(self, param):
        """Handler for the SLEEP command.

        This handler should probably not be overwriten in descendents.
        """
        time_str_fmt = "%Y%m%dT%H%M%S"
        now = time.strftime(time_str_fmt)
        wake_up_time = time.strftime(time_str_fmt, \
                                     time.localtime(time.time() + int(param)))
        logging.info("PING, sleeping for %s (now: %s wake: %s)",
                     str(param), now, wake_up_time)
        time.sleep(float(param))


######################################################################
# Example Clients
#
######################################################################


class PongClient(BaseClient):
    """Dummy client."""
    def __init__(self, client_id, base_url, store_dir=None):
        """DiggClient Constructor."""
        # Parent class constructor
        BaseClient.__init__(self, client_id, base_url, store_dir)
        # Registering Command Handlers
        self.handlers['PONG'] = self.pong

    def pong(self, _params):
        """Command handler that just prints pong in the screen."""
        print "PONG!"
        # Ok. Command, handled. Now what?
        # Since we don't contact the server after handling this
        # command, we just return from here -- there is no
        # need nor reason to call _handleCommand(..., do_sleep=True)


######################################################################
#
# Auxiliary functions
#
######################################################################


def getUUID(filename):
    """Retrieve a client id (UUID) from filename.
    
    If the file doesn't exists or if a UUID cannot be fond there, it will
    create one and store it there.

    Return:
        a client id (UUID).
    """
    # Try opening the file for reading
    try:
        fh = open(filename, 'r')
        client_id = fh.read()
        if len(client_id) < 36:
            # not a valid ID
            client_id = create_and_write_id(filename)
    except IOError:
        # File (probably) doesn't exist. Generate and ID and add it to the file
        client_id = create_and_write_id(filename)
    return client_id.strip()


def create_and_write_id(filename):
    """Create a new client ID (UUID), store it in filename and return it."""
    client_id = str(uuid.uuid1())
    fh = open(filename, 'w')
    fh.write(client_id)
    fh.close()
    return client_id


def log_backtrace():
    """Outputs to "log" a backtrace of the current exception.
    
    DEPRECATED - use logging.exception()
    """
    logging.exception("EXCEPTION in user code.")
    #log.write("EXCEPTION in user code:\n")
    #log.write('-'*60 + "\n")
    #traceback.print_exc(file=log)
    #log.write('-'*60 + "\n")
    #log.flush()


def log_urllib2_exception(exp):
    """Log a backtrace of the current URLLIB2 exception.
    
    Extra information from the exception is also output.
    """
    logging.exception("EXCEPTION in user code.")
    serv_err = ["EXCEPTION server response:\n",
                "-" * 60 + "\n",
                "\n".join([":".join([k, v]) for k, v in exp.info().items()]),
                "-" * 60 + "\n",
                str(exp.read()),
                "-" * 60 + "\n"]
    logging.error("\n\t".join(serv_err))

# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :
