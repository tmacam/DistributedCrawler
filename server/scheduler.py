# -*- coding: utf-8 -*-

#
# $Id: scheduler.py 207  2008-06-23 17:36:26 -0300 tmacam $
#
# Based on code in private repository:
#     scheduler.py 234 2006-11-14 01:16:46Z tmacam
#

"""Peridically assigns jobs to be processed."""


# We are not expecting more then 1k clients so, O(n) algorithms are
# more than enough.
# Remember:
#   - KISS!
#   - Premature optimization is the root of all evil.
#
#
# We don't put the peers in a queue or line. We just suggest an amount of time
# they must sleep to then contact us. If a buggy client keeps coming he will
# repeatly be used as long as there is work to do. That's the main idea: if the
# client comes to ping us and there's work to do, he will be given work to do.
#  It is the clients' job avoiding being caught crawling a site. Our job is to
# avoid DDOSing said site.
#
# We don't provide a stable storage for enqueued works. This must be performed
# by someone else. In our current implementation, this is how things are:
#
#  * scheduler's queues are volatile, i.e., must be repopulated every time a
#    server is restarted .
#
#  * Descendents from BaseControler (server.py) implement stable storange
#    mechanisms and are responsible for re-registering pending tasks with
#    the scheduler at every restart.


__all__ = ["Scheduler"]
__version__ = "0.3.posdigg"
__date__ = "2008-09-29 21:09:46 -0300 (Mon, 29 Sep 2008)"
__author__ = "Tiago Alves Macambira"
__copyright__ = "Copyright (c) 2006-2008 Tiago Alves Macambira"
__license__ = 'X11'


import time
from twisted.python import log

class Scheduler:
    """A "work" scheduler.

    This scheduler works by, at periodic "beats", moving one registered job from
    its "work" queue to its "ready" queue. Jobs in the "ready" queue can be
    claimed by outside entities, what will cause them being moved to the
    "active" queue.  Jobs that are in the "active" queue for too long will be
    reclaimed after a timeout and moved back to the end of the "work" queue.

    If none claims a job in the "ready" queue for more than two consecutive
    beats, it will be left at the queue. No more jobs will be moved to the
    "ready" queue if more then MAX_READY_WORKS are already there.

    About Peers and Clients
    -----------------------

    A "client" or peer is an abstraction for any entity that can claim jobs.
    This class only keeps information about known alive "peers" and it doesn't
    assign jobs to "peers": as long as there is work available in the "ready"
    queue, any peer will do.

    About PINGS and commands
    ------------------------

    This scheduler expects that peers contact (PING) it at periodic intervals.
    The length of this interval is calculated based on the ammount of known
    alive peers and is informed to peers everytime they contact the
    scheduler/server, be it just to "ping" it, or to inform of the completion of
    some job.

    As a result of a PING will be a "command", something of the form "<action>
    <parameters> #". The # is used to mark the end of the command. No whitespace
    is allowed as part of the action or parameter arguments. The default action
    informed by the scheduler is SLEEP, with a parameter informing the ammount
    of seconds the peers should wait before contacting the scheduler again.

    Class Atributes
    ---------------

    SLEEP_DELAY: Nodes will be asked to sleep for SLEEP_DELAY more seconds
        then needed, to avoid colision with the timer interval

    MAX_READY_WORKS: Max number of peding/enqued jobs that can be delivered
        to client processing at once. It's a balance between not
        wasting intervals where no job was assigned and not DDos'ins

    MIN_LIVENESS_INTERVALS: Number of intervals to wait before we assume
        a given active job failed. After this time we will "recycle" it,
        putting it back into the queue.

    MIN_LIVENESS_CYCLES: Number of cycles to wait before we declare a node
        as dead.  A cycle is the time it takes for every active node to have
        it's turn.
    """

    SLEEP_DELAY = 10
    MAX_READY_WORKS = 4
    MIN_LIVENESS_INTERVALS = 10
    MIN_LIVENESS_CYCLES = 2

    def __init__(self, interval=120, timer=None):
        """Scheduler constructror.

        Args:
            interval: seconds between server beets. The smaller the interval,
                the faster jobs will be moved between queues.

            timer: a twisted.internet.task.LoopingCall instance. This instance
                should have scheduler.timerCallback set at its target function.
                No arguments should be passed to the timerCallback. You can
                leave this parameter as None and just set i up after
                construction - there is support for this.
        """
        # Setup timer
        if timer is not None:
            self.timer = timer
        self.interval = interval
        self.next_interval = time.time()    # next beat should be... now!
        # Records the last ping of every "fresh" peer
        self.peers = {}
        # Setup queues
        self.ready_queue = []  # works ready to be processed
        self.work_queue = []   # works waiting to be processing
        self.active_queue = {} # works that assigned/being processed
                               # work as key, ts as value

    def renderPing(self, peer_id, just_ping=False):
        """Inform a peer what it should do, returning a command.
        
        Args:
            peer_id: The uniq identifier of the peer.
            
            just_ping: Should e PING be forced even if there is work available
                in the ready queue? Just might be the case if the peers just
                pinged to inform/submit the completion of an assigned job.

        Returns:
            A command, as informed in this class's documentation.
        """
        # Refresh peer liveness timestamp
        now = time.time()
        self.peers[peer_id] = now
        n_peers = len(self.peers) - 1
        next_turn = (self.next_interval - now) + (n_peers * self.interval)
        # "Render" the command
        if len(self.ready_queue) > 0 and not just_ping:
            # Got work to do
            return self._assignWork(peer_id)
        else:
            # The End
            return "SLEEP %i #" % (next_turn + self.SLEEP_DELAY)


    def _assignWork(self, peer_id):
        """Assign an avaiable job to a peer.

        Args:
            peer_id: The peer's uniq identifier.

        Returns:
            A command to be returned to the peer.
        """
        log.msg( "Assigning work to peer-id " + peer_id )
        work = self.ready_queue.pop()
        self.active_queue[work] = time.time()
        action, params = work
        return "%s %s #" % (action, params)

    def appendWork(self, action, params):
        """Enqueue a work for future processing.
        
        Args:
            action: action to be submited to the user. Should be a string and
                must not have any whitespace in it.

            params: params for the given action. Should be a string and
                must not have any whitespace in it.
        """
        self.work_queue.append((action, params))

    def timerCallback(self):
        """Update timers, schedule more jobs, rescue jobs that got stucked and
        clean dead peers.
        
        This function should be called periodically at self.interval seconds.
        See this class constructor for more information on how to do this.
        """
        # Update timers
        now = time.time()
        liveness_threshold = now - (self.MIN_LIVENESS_INTERVALS * self.interval)
        self.next_interval = now + self.interval
        # Deal with enqueued jobs
        if self.work_queue and len(self.ready_queue) <= self.MAX_READY_WORKS:
            self.ready_queue.append(self.work_queue.pop())
        for work, timestamp in self.active_queue.items():
            if timestamp < liveness_threshold:
                # Recycle this work. We use work_queue as a FIFO "stack":
                # we pop() from its END and we add "new" items to its START
                del self.active_queue[work]
                self.work_queue.insert(0, work)
        # Remove dead nodes
        node_liveness_threshold = now - (self.MIN_LIVENESS_CYCLES *
                                         self.interval * 
                                         len(self.peers))
        for peer, timestamp in self.peers.items():
            if timestamp < node_liveness_threshold:
                del self.peers[peer]

    def markWorkDone(self, action, params):
        """Mark a job as done, i.e., remove work from all known lists."""
        work = (action, params)
        if work in self.active_queue :
            del self.active_queue[work]
        elif work in self.work_queue:
            self.work_queue.remove(work)
        elif work in self.ready_queue:
            self.ready_queue.remove(work)
        else:
            msg = "Unknown work being marked as done: " +  str(work)
            log.err(msg)
            raise KeyError(msg)

    # Timer control methods
    def start(self):
        """Start timer (if set on initialization)."""
        if self.timer:
            self.timer.start(self.interval)

    def stop(self):
        """Stop timer (if set on initialization)."""
        if self.timer:
            self.timer.stop()

    def reschedule(self, new_interval):
        """Sets a new interval value (in seconds) and reschedule the 
        timer accordingly.
        """
        self.interval = new_interval
        if self.timer:
            self.timer.stop()
            self.timer.start(new_interval)


# vim: set ai tw=80 et sw=4 ts=4 sts=4 fileencoding=utf-8 :
