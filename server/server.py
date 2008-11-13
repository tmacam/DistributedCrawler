#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Basic infra-estructure and classes for a DistributedCrawler server.

This module provides the basic framework for constructing the sever-side part of
a distributed crawler. This is a HTTP-base distributed framework i.e., clients
communicate with the server using HTTP.

The process of crawling the contents of a site can be compromised of many
different classes tasks: collecting index pages, discovering article pages,
downloading articles, downloading comments etc.  For each of these classes of
"tasks" one Task Controller should be created.  Tasks controllers are
responsible for:
    * Storing the list of pending, done and erroneus jobs known to it,
    * Submiting pending jobs for client processing (by using a scheduler. More
      on this latter),
    * Processing client responses (as result of job assignments)

Most of this functionalyity can be inheritad from BaseControler, which also
provides the needed funcionality to register the TaskControler with the HTTP
server "path-space" (thus making the task server-side code acessible to clients)
and providing some basic status report functionality.

Jobs (works) management is handled by a DistributedCrawler.server.scheduler
instance. Since scheduler instances don't provide persistent/safe storage for
known tasks, this must be done by Tasks Controlers. The scheduler itself is
task-agnostic: it only understands "commands", i.e., "ACTION parameter" pairs.
Usually, for everty Task Controler there will be a corresponding ACTION.

Since we use twisted as our HTTP server framework, some knowledge of twisted.web
"lingo" is suggested.
"""

__version__ = "0.3.posdigg"
__date__ = "2008-09-29 21:09:46 -0300 (Mon, 29 Sep 2008)"
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'

import time
import os

from twisted.web import server, resource, http
from twisted.python import log
from twisted.python.logfile import DailyLogFile
from twisted.persisted.dirdbm import DirDBM

import scheduler
sched_version = scheduler.__version__ 

########################################################################
#
# This is Generic stuf
#
########################################################################


class InvalidClientId(Exception):
    """Signals that an invalid, malformed of missing ClientID was seen."""
    pass


class ManageScheduler(resource.Resource):
    """Basic management and stats.  interface for DistributedCrawler servers.
    
    This page (resource) provide a simple way to control a DistributedCrawler
    server and to retrieve information about the crawling process: jobs done,
    pending, etc.
    
    Information about jobs are retrieved directly from the scheduler and from
    controlers for specific tasks.
    """
    stats_html = """<html>
    <head><title>Manage Scheduler Parameters</title></head>
    <body>
    <h1>Current Settings</h1>
    <dl>
        <dt>Interval</dt><dd>%(interval)i seconds</dd>
    </dl>
    <form action="manage" method="post">
        New Interval: <input type="text" name="interval" />
        <input type="submit" value="Update"/>
    </form>
    <h1>Scheduler Status</h1>
    <dl>
        <dt>Next Interval</dt><dd>%(next_interval_in)02.2f seconds</dd>
        <dt>Ready jobs</dt><dd>%(ready)i</dd>
        <dt>Active jobs</dt><dd>%(active)i</dd>
        <dt>Queued jobs</dt><dd>%(queued)i</dd>
        <dt>Active Clients</dt><dd>%(n_clients)i</dd>
    </dl>
    %(other_services)s
    <p><small> Server v.%(serv_version)s /
               Scheduler v.%(sched_version)s </small></p>
    </body>
    </html> """

    def __init__(self, scheduler, timer, other_services={}):
        """ManageScheduler constructor.
        
        Args:
            scheduler: A DistributedCrawler.server.scheduler instance.

            timer: a twisted.internet.task.LoopingCall instance. This MUST be
                the same instance used for the scheduler construction.

            other_services: a dict of Task Controlers, where the keys are the
                tasks' names and values' their instances.
        """
        resource.Resource.__init__(self)
        self.scheduler = scheduler
        self.timer = timer
        self.other_services = other_services

    def _getOtherServicesStatus(self):
        """Return the HTML code reporting the status of known Task Controlers."""
        buf = []
        for name, serv  in self.other_services.items():
            buf.append('<h1>%s Status</h1>\n%s\n' % (name, serv.getStatus()))
        return ''.join(buf)

    def render(self, request):
        """Render HTML code for the ManageScheduler page"""
        if request.args.has_key('interval'):
            interval = int(request.args['interval'][0])
            self.scheduler.reschedule(interval)
        now = time.time()
        stats = {   'interval' : self.scheduler.interval,
                    'next_interval_in': self.scheduler.next_interval - now,
                    'ready': len(self.scheduler.ready_queue),
                    'active': len(self.scheduler.active_queue),
                    'queued': len(self.scheduler.work_queue),
                    'n_clients': len(self.scheduler.peers),
                    'other_services' : self._getOtherServicesStatus(),
                    'serv_version': __version__,
                    'sched_version' : sched_version,
                }
        return self.stats_html % stats
        

class Ping(resource.Resource):
    """Handles client's periodic contact request and dispatches jobs.
    
    Clients (peers) periodically contact the server to inform it they are alive
    and to request work to do. This resource handles this contact request.

    @warning: most clients expect to find this resource in the "/ping" path.
    """ 
    def __init__(self,scheduler, client_reg):
        """Constructor.

        Args:
            scheduler: a DistributedCrawler.scheduler instance.
            
            client_reg: A ClientRegistry instance.
        """
        resource.Resource.__init__(self)
        self.scheduler = scheduler
        self.client_reg = client_reg

    def render(self,request):
        """Render the command that should be returned to the client."""
        client_id = self.client_reg.updateClientStats(request)
        return self.scheduler.renderPing(client_id)


class BaseControler(resource.Resource):
    """Base funcionality of a Task Controller.

    Controllers are responsable for managing particular classes of tasks,
    keeping them in stable storage, loading and registering them with the
    scheduler at start-up and processing clients reponses for said tasks.

    Managing with jobs are pending, marking them as done (and removing 'em from
    the scheduler) and keeping track of erroneus jobs is controller
    responsability as well -- most of it is implemented here, in BaseControler.

    Clients will contact a controller to return the resulting of a dispached
    job through the HTTP web-server, which in turn will call the controller's
    render_POST method. The controller should process the client response
    acordingly.


    NOTICE: For subclasses you will have to set:
        * the following class attributes:
            - PREFIX_BASE
            - ACTION_NAME
        * the following methods:
            - render_POST
    """

    STATUS_HTML = """<dl>
            <dt>Queued jobs</dt><dd>%(queued)i (%(queued_percent)02.02f%%)</dd>
            <dt>Done jobs</dt><dd>%(done)i (%(done_percent)02.02f%%)</dd>
            <dt>Erroneus jobs</dt><dd>%(err)i (%(err_percent)02.02f%%)</dd>
            <dt>Total</dt><dd>%(total)i</dd>
        </dl>"""

    def __init__(self, scheduler, prefix, client_reg):
        """Constructor.

        Args:
            scheduler: A scheduler instance. Jobs will be added to it.
            prefix: the base path where the persistent storage will be
                created/read.
            client_reg: Client Registry instance.
        """
        resource.Resource.__init__(self)
        # Set things up
        self.scheduler = scheduler
        self.client_reg = client_reg 
        self.store_path = prefix + "/" + self.PREFIX_BASE + "/"
        # Setup stores
        queue_store_path = self.store_path + "/queue"
        done_store_path = self.store_path + "/done"
        err_store_path = self.store_path + "/error"
        for p in [queue_store_path, done_store_path, err_store_path]:
            if not os.path.isdir(p):
                os.makedirs(p)
        self.store = DirDBM(queue_store_path)
        self.done_store = DirDBM(done_store_path)
        self.err_store = DirDBM(err_store_path)
        # Load previous stored data
        for job in self.store.keys():
            self._addToScheduler(job)

    def _addToScheduler(self, job):
        self.scheduler.appendWork(self.ACTION_NAME, job)

    def _addToStore(self, job):
        self.store[job] = '1'

    def addJob(self, job):
        if (job not in self.done_store) and (job not in self.store):
            self._addToStore(job)
            self._addToScheduler(job)

    def markJobAsDone(self, job):
        # Add to done store
        self.done_store[job] = '1'
        # Remove job from the local's and from scheduler's queue
        if job in self.store:
            del self.store[job]
        self.scheduler.markWorkDone(self.ACTION_NAME, job)

    def markJobAsErroneus(self, job):
        # This job does exist, right? 
        if not self.store.has_key(job):
            raise KeyError("Unknown job " + str(job))
        # Add this job to the error store
        self.err_store[job] = '1'
        # Remove job from the local's and from scheduler's queue
        if job in self.store:
            del self.store[job]
        self.scheduler.markWorkDone(self.ACTION_NAME, job)

    def getChild(self, path, request):
        return self

    def getStatus(self):
        queued = len(self.store)
        done = len(self.done_store)
        total = queued + done 
        status= {'queued' : queued, 'done': done, 'total':total, \
                    'queued_percent': (queued * 100)/total,
                    'done_percent': (done * 100)/total,
                    }
        return self.STATUS_HTML % status 

class ArticleControler(BaseControler):
    ACTION_NAME = "ARTICLE"
    PREFIX_BASE = "articles"

    def __init__(self, scheduler, prefix, client_reg, store_dir):
        """
        @param store_dir where the articles (compressed) will be stored.
        """
        BaseControler.__init__(self, scheduler, prefix, client_reg)
        # Try to create this directory if it doesn't exist
        if not os.path.isdir(store_dir):
            os.makedirs(store_dir)
        self.store_dir = store_dir

    def render_POST(self,request):
        client_id = self.client_reg.updateClientStats(request)
        # get the articleId
        article_sid = request.args['article-sid'][0]
        article_data = request.args['article-data'][0]
        # save the contents of the article
        escaped_sid = article_sid.replace('/','_')
        fh_filename = os.path.join(self.store_dir,escaped_sid + '.xml.gz')
        fh = open(fh_filename,'wb')
        fh.write(article_data)
        fh.close()
        # Ok! Article saved!
        self.markJobAsDone(article_sid)
        log.msg("ARTICLE %s done by client %s." % (article_sid, client_id) )
        return self.scheduler.renderPing(client_id, just_ping=True)


class ClientRegistry(resource.Resource):
    """
    Information about client is stored in a DirDBM. For a given client ID,
    we store it's CLIENT_SENT_HEADERS headers and the # of jobs performed.
    Information is stored as a string, fields separated by '#'.
    """

    isLeaf = True

    CLIENT_SENT_HEADERS = ['client-id','client-hostname','client-version',\
                           'client-arver']

    HTML_HEADER = """<html>
        <head>
            <title>Client Status</title>
            <link href="../static/style.css" type="text/css" rel="stylesheet" />
        </head>
        <body>
        <h1>Clients</h1>
        <table class="clientState">
         <thead>
           <tr>
             <th>client-hostname</th><th>client-version</th>
             <th>client-arver</th><th># jobs</th><th>state</th><th>Next job</th>
           </tr>
         <thead>
         <tbody>"""
    HTML_FOOTER="""</tbody></table>
        </body>
        </html> """


    def __init__(self, scheduler, prefix):
        """
        @param scheduler A scheduler instance. Job will be added to it.
        @param prefix the base path where the store will be created/read.
        """
        resource.Resource.__init__(self)
        # Set things up
        self.scheduler = scheduler
        self.store_path = prefix + "/clients/"
        if not os.path.isdir(self.store_path):
            os.makedirs(self.store_path)
        self.known_clients = DirDBM(self.store_path)
        self.jobs_done = {}
        for id in self.known_clients.keys():
            self.jobs_done[id] = int(self.known_clients[id].split("#")[4])

    def updateClientStats(self, request,job_done=False):
        """Updates information about a client.
        
        @return client's client_id.
        """
        client_id = request.getHeader('client-id')
        if client_id is None:
            raise InvalidClientId()
        if job_done:
            self.jobs_done[client_id] = self.jobs_done.get(client_id,0) + 1
        client_data = []
        for hdr in self.CLIENT_SENT_HEADERS:
            content = request.getHeader(hdr)
            if not content:
                content = 'UNKNOWN'
            client_data.append(content)
        client_data.append(str(self.jobs_done.get(client_id,0)))
        self.known_clients[client_id] = "#".join(client_data)

        return client_id

    def render(self,request):
        now = time.time()
        result = []
        result.append(self.HTML_HEADER)
        for client_id in self.known_clients.keys():
            # get client status
            last_seen = self.scheduler.peers.get(client_id)
            if last_seen:
                state = 'ALIVE'
            else:
                state='DEAD'
                last_seen = now + 1 # Avoid a (None - Float) subtracion bellow 
            # print client data
            result.append('<tr class="%s" id="%s">' % (state,client_id))
            for val in self.known_clients[client_id].split("#")[1:]:
                result.append('<td>%s</td>'%val)

            result.append('<td>%s</td>' % state)
            result.append('<td>%i</td>' % (now - last_seen))
            result.append('</tr>')
        result.append(self.HTML_FOOTER)
        return "".join(result)
        
                

if __name__ == '__main__':
    from twisted.internet import reactor, task
     
    print "\nIniciando server...\n"

    PORT = 8700
    PREFIX = './db/'
    ARTICLE_STORE_DIR = '/home/digg/article_archive/'
    INTERVAL = 60

    # Setup logging
    logfile = DailyLogFile('diggcrawler.log','.')
    log.startLogging(logfile)

    scheduler = scheduler.Scheduler(INTERVAL)

    sched_timer = task.LoopingCall(scheduler.timerCallback)
    scheduler.timer = sched_timer
    scheduler.start()

    root = resource.Resource()
    # Main services' resources
    client_reg = ClientRegistry(scheduler,PREFIX)
    article_controler = ArticleControler(scheduler, PREFIX, client_reg, ARTICLE_STORE_DIR)
    
    root.putChild('ping', Ping(scheduler,client_reg))
    root.putChild('clients',client_reg)
    root.putChild('article', article_controler)
    
    other_service = {'Articles': article_controler}
    root.putChild('manage', ManageScheduler(scheduler, sched_timer, other_service))
    
    site=server.Site(root)

    reactor.listenTCP(PORT, site)

    reactor.run()


# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :