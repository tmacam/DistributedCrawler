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

Jobs (works) management is handled by a scheduler.Scheduler
instance. Since Scheduler instances don't provide persistent/safe storage for
known tasks, this must be done by Tasks Controlers. The scheduler itself is
task-agnostic: it only understands "commands", i.e., "ACTION parameter" pairs.
Usually, for everty Task Controler there will be a corresponding ACTION.

Since we use twisted as our HTTP server framework, some knowledge of twisted.web
"lingo" is suggested.

Logging is handled by twisted.python.log.
"""

__version__ = "0.3.posdigg"
__date__ = "2008-09-29 21:09:46 -0300 (Mon, 29 Sep 2008)"
__author__ = "Tiago Alves Macambira"
__copyright__ = 'Copyright (c) 2006-2008 Tiago Alves Macambira'
__license__ = 'X11'

import gdbm
import time
import os

from twisted.web import server, resource
from twisted.internet import reactor, task
from twisted.python import log
from twisted.persisted.dirdbm import DirDBM

import scheduler


######################################################################
# Auxiliary classes and functions
######################################################################


class OldMappingIteratorProxy(object):
    """Enhances old mapping-like objects by adding PEP-234 iteration.
    
    This class implements the Proxy pattern for mapping-like objects. It
    enhances those objects by adding the necessary methods to make those
    objects PEP-0234-friendly, i.e., by addinhg a __iter__() and
    __contains__() method for those objects.
    """
    def __init__(self, target):
        """Constructor.

        @param target The object to be "enhanced".
        """
        self.__target = target

    def __getattr__(self, name):
        return getattr(self.__target, name)

    def __contains__(self, key):
        "Allows efficient use of the 'in' operator."
        return bool(self.__target.has_key(key))

    def __iter__(self):
        "Allows plain 'for i in container: ...'."
        return iter(self.__target.keys())


######################################################################
# Main classes and Functions
######################################################################


class InvalidClientId(Exception):
    """Signals that an invalid, malformed or missing ClientID was seen."""
    pass


class ManageScheduler(resource.Resource):
    """Basic management and status interface for DistributedCrawler servers.
    
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

    def __init__(self, sched, timer, other_services=None):
        """ManageScheduler constructor.
        
        Args:
            sched: A DistributedCrawler.server.scheduler.Scheduler instance.

            timer: a twisted.internet.task.LoopingCall instance. This MUST be
                the same instance used for the scheduler construction.

            other_services: a dict of Task Controlers, where the keys are the
                tasks' names and values' their instances.
        """
        resource.Resource.__init__(self)
        self.scheduler = sched
        self.timer = timer
        if other_services is None:
            other_services = {}
        self.other_services = other_services

    def _getOtherServicesStatus(self):
        """Return HTML code reporting the status of known Task Controlers."""
        buf = []
        for name, serv  in self.other_services.items():
            buf.append('<h1>%s Status</h1>\n%s\n' % (name, serv.getStatus()))
        return ''.join(buf)

    def render(self, request):
        """Render HTML code for the ManageScheduler page."""
        if request.args.has_key('interval'):
            interval = int(request.args['interval'][0])
            self.scheduler.reschedule(interval)
        now = time.time()
        sched_version = scheduler.__version__
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

    def registerTaskController(self, controller, name):
        """Register a task controller with the status reporting interface.
        
        Args:
            controler: a task controller instance (descendent of BaseControler).

            name: Name under which this task will be listed in the '/manage'
                status page.

        @warning We don't check for duplicates.
        """
        self.other_services[name] = controller
        

class Ping(resource.Resource):
    """Handles client's periodic contact request and dispatches jobs.
    
    Clients (peers) periodically contact the server to inform it they are alive
    and to request work to do. This resource handles this contact request.

    @warning: most clients expect to find this resource in the "/ping" path.
    """
    def __init__(self, sched, client_reg):
        """Constructor.

        Args:
            sched: a DistributedCrawler.server.scheduler.Scheduler instance.
            
            client_reg: A ClientRegistry instance.
        """
        resource.Resource.__init__(self)
        self.scheduler = sched
        self.client_reg = client_reg

    def render(self, request):
        """Render the command that should be returned to the client."""
        client_id = self.client_reg.updateClientStats(request)
        return self.scheduler.renderPing(client_id)


class BaseControler(resource.Resource):
    """Base funcionality of a Task Controller.

    Controllers are responsible for managing particular classes of tasks,
    keeping state information in stable storage, loading and registering
    pending tasks with the scheduler at start-up and processing clients'
    reponses for said tasks.

    Managing which jobs are pending, marking them as done (and removing 'em from
    the scheduler) and keeping track of erroneus jobs is a controller
    responsability as well -- most of it is implemented here, in BaseControler.

    Clients will contact a controller to return the results of a dispached
    job through the HTTP web-server, which in turn will call the controller's
    render_POST method. The controller should process clients' responses
    acordingly.


    NOTICE: For subclasses you will have to set:
        * the following class attributes:
            - PREFIX_BASE
            - ACTION_NAME
        * the following methods:
            - render_POST

    NOTICE: This implementation uses twisted's DirDBM as stable storage
            mechanism. To alter this overwrite setupStableStorage(). For
            now we expect the storage mechanism to export a mapping
            (dictionary-like) interface. This may change (be refactored)
            in the future. Also, we used "not in xxx.keys()" instead of
            the simpler "not in xxx" because other DBM interfaces like
            gdbm don't provide support for this simpler interface.
    """

    STATUS_HTML = """<dl>
            <dt>Queued jobs</dt><dd>%(queued)i (%(queued_percent)02.02f%%)</dd>
            <dt>Done jobs</dt><dd>%(done)i (%(done_percent)02.02f%%)</dd>
            <dt>Erroneus jobs</dt><dd>%(err)i (%(err_percent)02.02f%%)</dd>
            <dt>Total</dt><dd>%(total)i</dd>
        </dl>"""

    def __init__(self, sched, prefix, client_reg):
        """Constructor.

        Args:
            sched: A DistributedCrawler.server.scheduler.Scheduler instance.
                Jobs will be added to it.

            prefix: the base path where the persistent storage will be
                created/read.

            client_reg: a ClientRegistry instance.
        """
        resource.Resource.__init__(self)
        # Set things up
        self.scheduler = sched
        self.client_reg = client_reg
        # Setup stores
        self.store_path = prefix + "/" + self.PREFIX_BASE + "/"
        self.setupStableStorage()
        # Load previously stored data
        for job in self.store.keys():
            self._addToScheduler(job)

    def setupStableStorage(self):
        """Setup stable storage used by this BaseControler.
        
        Load and setup stable storage mechanism for the pending, done and
        erroneous task queues.

        By default we used twisted's DirDBM, creating the directories where
        the queues will be stored if needed.
        """
        # Setup stores
        queue_store_path = self.store_path + "/queue"
        done_store_path = self.store_path + "/done"
        err_store_path = self.store_path + "/error"
        for queue_path in [queue_store_path, done_store_path, err_store_path]:
            if not os.path.isdir(queue_path):
                os.makedirs(queue_path)
        self.store = DirDBM(queue_store_path)
        self.done_store = DirDBM(done_store_path)
        self.err_store = DirDBM(err_store_path)

    def _addToScheduler(self, job):
        """Register a pending job with the scheduler."""
        self.scheduler.appendWork(self.ACTION_NAME, job)

    def _addToStore(self, job):
        """Register a pending job in the persistent storage."""
        self.store[job] = '1'

    def addJob(self, job):
        """Register a (probably new and unknown) job with this Controller."""
        if job not in self.done_store and job not in self.store:
            self._addToStore(job)
            self._addToScheduler(job)

    def markJobAsDone(self, job):
        """Mark a job as done and remove it from "pending" queues."""
        # Add to done store
        self.done_store[job] = '1'
        # Remove job from the local's and from scheduler's queue
        if job in self.store:
            del self.store[job]
        self.scheduler.markWorkDone(self.ACTION_NAME, job)

    def markJobAsErroneus(self, job):
        """Dequeue job and save it in the (persistent) list of erroneus jobs.
        
        Erroneus jobs are jobs that, for some reason, were flagged by clients as
        being probelattic to handle.
        """
        # This job does exist, right?
        if job not in self.store:
            raise KeyError("Unknown job " + str(job))
        # Add this job to the error store
        self.err_store[job] = '1'
        # Remove job from the local's and from scheduler's queue
        if job in self.store:
            del self.store[job]
        self.scheduler.markWorkDone(self.ACTION_NAME, job)

    def getChild(self, _path, _request):
        """Retrieve a 'child' resource from me.
        
        The default behaviour for BaseControler is to return itself as handler
        for child resources. This allows for using the "path" of this resource
        as a mean for passing extra information to the Controller. For instance,
        you could encode the job identifier in the request path.
        """
        return self

    def getStatus(self):
        """Return the HTML code reporting the status of this Task Controller."""
        queued = len(self.store)
        done = len(self.done_store)
        err = len(self.err_store)
        total = queued + done + err
        if total == 0.0 :
            queued_percent = 0.0
            done_percent = 100.0
            err_percent = 0.0
        else:
            queued_percent = (queued * 100.0)/total
            done_percent = (done * 100.0)/total
            err_percent = (err * 100.0)/total
        status = {'queued' : queued,
                  'done' : done,
                  'err' : err,
                  'total' : total,
                  'queued_percent' : queued_percent,
                  'done_percent' : done_percent,
                  'err_percent' : err_percent }
        return self.STATUS_HTML % status


class GdbmBaseControler(BaseControler):
    """A BaseControler that uses GDBM as stable storage mechanism."""
    def setupStableStorage(self):
        """Setup stable storage used by this BaseControler. """
        # Setup stores
        queue_store_path = self.store_path + "/queue.gdbm"
        done_store_path = self.store_path + "/done.gdbm"
        err_store_path = self.store_path + "/error.gdbm"
        # Make dirs
        if not os.path.isdir(self.store_path):
            os.makedirs(self.store_path)
        # Load for syncrhonized read and write, creating the DBs if necessary
        self.store = gdbm.open(queue_store_path, "cs")
        self.done_store = gdbm.open(done_store_path, "cs")
        self.err_store = gdbm.open(err_store_path, "cs") 
        # Update GDBM API by using OldMappingIteratorProxy
        self.store = OldMappingIteratorProxy(self.store)
        self.done_store = OldMappingIteratorProxy(self.done_store)
        self.err_store = OldMappingIteratorProxy(self.err_store)
        # clean DBs before usage
        for db in (self.store, self.done_store, self.err_store):
            db.reorganize()


class ClientRegistry(resource.Resource):
    """Keeps information about the clients that contacted this server.

    Besides storing information about our peers (clients), this class also
    provides a page (resource) from with information about our clients (name,
    ID, IP, number of jobs completed) can be gathered.

    Information about client is stored in a DirDBM. For a given client ID,
    we store it's CLIENT_SENT_HEADERS headers and the # of jobs performed.
    Information is stored as a string, fields separated by '#'.
    """

    isLeaf = True

    CLIENT_SENT_HEADERS = ['client-id', 'client-hostname', 'client-version', \
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

    HTML_FOOTER = """</tbody></table>
        </body>
        </html> """

    def __init__(self, sched, prefix):
        """ClientRegistry Constructor.

        Args:
            sched: A scheduler.Scheduler instance.

            prefix: The base path where the store will be created/read.
        """
        resource.Resource.__init__(self)
        # Set things up
        self.scheduler = sched
        self.store_path = prefix + "/clients/"
        # Setup/restore persistent storage for client information
        if not os.path.isdir(self.store_path):
            os.makedirs(self.store_path)
        self.known_clients = DirDBM(self.store_path)
        # Restore information about jobs done
        self.jobs_done = {}
        for client_id in self.known_clients.keys():
            num_jobs_done = int(self.known_clients[client_id].split("#")[4])
            self.jobs_done[client_id] = num_jobs_done

    def updateClientStats(self, request, job_done=False):
        """Updates information about a client and retrieve its id.

        This method should be called by Task Controllers in order to keep our
        knowledge about clients fresh and correct.

        Args:
            request: The request object passed by the twisted framework. Client
                identification will be extracted from it.

            job_done: Was a task/job completed by this client? Pass True if so,
                or False if this was just a ping or something related.

        @return client's client_id.
        """
        client_id = request.getHeader('client-id')
        if client_id is None:
            raise InvalidClientId()
        if job_done:
            self.jobs_done[client_id] = self.jobs_done.get(client_id, 0) + 1
        # store client information in persistent storage
        client_data = []
        for hdr in self.CLIENT_SENT_HEADERS:
            content = request.getHeader(hdr)
            if not content:
                content = 'UNKNOWN'
            client_data.append(content)
        client_data.append(str(self.jobs_done.get(client_id, 0)))
        self.known_clients[client_id] = "#".join(client_data)

        return client_id

    def render(self, _request):
        """Render HTML code for the client status page."""
        now = time.time()
        result = []
        result.append(self.HTML_HEADER)
        for client_id in self.known_clients.keys():
            # get client status
            last_seen = self.scheduler.peers.get(client_id)
            if last_seen:
                state = 'ALIVE'
            else:
                state = 'DEAD'
                last_seen = now + 1 # Avoid a (None - Float) subtracion bellow
            # print client data
            result.append('<tr class="%s" id="%s">' % (state, client_id))
            for val in self.known_clients[client_id].split("#")[1:]:
                result.append('<td>%s</td>' % val)

            result.append('<td>%s</td>' % state)
            result.append('<td>%i</td>' % (now - last_seen))
            result.append('</tr>')
        result.append(self.HTML_FOOTER)
        return "".join(result)


class BaseDistributedCrawlingServer:
    """Simple class that handles most of the twited setup code.
    
    Linking all the pieces that make a DistributedCrawler server togueter is
    rather borring -- and pretty much copy and past of of the time. This class
    tried to fix this issue by doing most of the boring setup itself. All you
    gotta do is instanciate and register your Task Handlers and call it's run
    method.
    """

    def __init__(self, port=8700, prefix='./db/', interval=60):
        """Constructor.
        
        Args:
            port: (int) port where we will be listening for HTTP conections.

            prefix: the base path where the persistent storage will be
                created/read.

            interval: (int) seconds between scheduler beats.
        """
        # Store config locally
        self.port = port
        self.prefix = prefix
        self.interval = interval
        # Setup Scheduler instance
        self.scheduler = scheduler.Scheduler(self.interval)
        sched_timer = task.LoopingCall(self.scheduler.timerCallback)
        self.scheduler.timer = sched_timer
        self.scheduler.start()
        # Main server resources
        self.root = resource.Resource()
        self.client_reg = ClientRegistry(self.scheduler, self.prefix)
        self.root.putChild('clients', self.client_reg)
        self.root.putChild('ping', Ping(self.scheduler, self.client_reg))
        self.task_manager_ui = ManageScheduler(self.scheduler, sched_timer)
        self.root.putChild('manage', self.task_manager_ui)
        self.terminate = TerminateServerResource()
        self.root.putChild('quitquitquit', self.terminate)

    def getScheduler(self):
        """Get the Scheduler instance used by the server."""
        return self.scheduler

    def getClientRegistry(self):
        """Get the ClientRegistry used by the server."""
        return self.client_reg

    def registerTaskController(self, controller, path, name):
        """Register a Task Controller with this server.
        
        The controller will be extenally accessible and will be
        registered with the '/manage' status page.

        Args:
            controler: a task controller instance (descendent of BaseControler).

            path: path bellow which this task controller will be externally
                accessible.

            name: Name under which this task will be listed in the '/manage'
                status page.
        """
        log.msg("Registering controler '%s' in path '%s'" % (name, path))
        self.root.putChild(path, controller)
        self.task_manager_ui.registerTaskController(controller, name)


    def run(self):
        """Start handling connections and events."""
        log.msg("Starting twisted reactor")
        site = server.Site(self.root)
        reactor.listenTCP(self.port, site)
        reactor.run()


class TerminateServerResource(resource.Resource):
    "A resource whose only purpose is to gracefully shut the server down."

    def render(self, request):
        "Just stop the server."
        reactor.stop()
        return "Exiting"

# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :
