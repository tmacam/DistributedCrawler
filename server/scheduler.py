# -*- coding: utf-8 -*-

"""
We don't put the peers in a queue or line. We just suggest an ammount of time
they must sleep to then contact us. If a buggy client keeps coming he will 
repeatly be used as long as there work to do. That's the main idea: if the
client comes to ping and there's work to do, he will have to work. Is their
job avoiding being caught crawling at - Our job is to avoid DDos'ing - 
"""

__all__ = ['Scheduler']
__version__ = "$Revision: 234 $".split()[1]


import time
from twisted.python import log

class Scheduler:

    """ 
    Class Atributes     
    ---------------
    SLEEP_DELAY: Nodes will be asked to sleep for SLEEP_DELAY more seconds
                 then needed, to avoid colision with the timer interval
    
    MAX_READY_WORKS: Max number of peding/enqued jobs that can be delivered
                 to client processing at once. It's a balance between not
                 wasting intervals where no job was assigned and not DDos'ins

    MIN_LIVENESS_INTERVALS: Number of intervals to wait before we assume a given
                 active job failed. After this time we will "recycle" it, putting
                 it back into the queue.

    MIN_LIVENESS_CYCLES: Number of cycles to wait before we declare a node as dead.
                 A cycle is the time it takes for every active node to have it's turn 
    """

    SLEEP_DELAY = 10
    MAX_READY_WORKS = 4
    MIN_LIVENESS_INTERVALS = 10
    MIN_LIVENESS_CYCLES = 2

    def __init__(self,interval=120,timer=None):

        if timer is not None:
            self.timer = timer

        self.interval = interval
        #now
        self.next_interval = time.time() 

        # recors the last ping of every "fresh" peer
        self.peers={} 

        self.ready_queue = []  # works ready to be processed
        self.work_queue = []   # works waiting to be processing
        self.active_queue = {} # works that assigned/being processed 
                               # work as key, ts as value
                                   

    def renderPing(self, id, just_ping=False):

        # Refresh peer liveness timestamp
        now = time.time()
        self.peers[id] = now
        n_peers = len(self.peers) - 1
        next_turn = (self.next_interval - now) + n_peers*self.interval

        if len(self.ready_queue) > 0 and not just_ping:
            # Got work to do 
            return self._assignWork(id)
        else:
            # The End
            return "SLEEP %i #" % (next_turn + self.SLEEP_DELAY)


    def _assignWork(self, id):
        log.msg( "Assigning work to peer-id " + id )
        work = self.ready_queue.pop()
        self.active_queue[work] = time.time()

        action, params = work
        return "%s %s #" % (action, params)

    def appendWork(self,action,params):
        """Enqueue a work for future processing."""
        self.work_queue.append((action,params))

    def timerCallback(self):
        """Update timers, schedule more jobs, rescue jobs that got stucked and
        clean dead peers."""

        now = time.time()
        liveness_threshold = now - self.MIN_LIVENESS_INTERVALS * self.interval

        self.next_interval = now + self.interval

        # Deal with enqueued jobs
        if self.work_queue and len(self.ready_queue) <= self.MAX_READY_WORKS:
            self.ready_queue.append( self.work_queue.pop())
        for work, ts in self.active_queue.items():
            if ts < liveness_threshold:
                del self.active_queue[work]
                self.work_queue.insert(0, work)
        
        # Remove dead nodes
        node_liveness_threshold = now - (self.MIN_LIVENESS_CYCLES * self.interval * len(self.peers))
        for p, ts in self.peers.items():
            if ts < node_liveness_threshold:
                del self.peers[p]

    def markWorkDone(self,action,params):
        # Remove work from lists
        work = (action,params)
        if work in self.active_queue :
            del self.active_queue[work]
        elif work in self.work_queue:
            self.work_queue.remove(work)
        elif work in self.ready_queue:
            self.ready_queue.remove(work)
        else:
            raise KeyError("Que diabo de trabalho eh esse: " + str(work))


    # Timer control methods
    def start(self):
        """Start timer (if set on initialization)."""
        if self.timer:
            self.timer.start(self.interval)

    def stop(self):
        """Stop timer (if set on initialization)."""
        if self.timer:
            self.timer.stop()

    def reschedule(self,new_interval):
        """Sets a new interval value and reschedule the timer accordingly."""
        self.interval = new_interval
        if self.timer:
            self.timer.stop()
            self.timer.start(new_interval)


# vim: set ai tw=80 et sw=4 sts=4 fileencoding=utf-8 :
