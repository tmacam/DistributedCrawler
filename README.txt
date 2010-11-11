



===================
Distributed Crawler
===================


Distributed Crawler is a collection of python classes (actually, it is a python module) whose aim is to aid the construction of stateless distributed crawlers. It was used to build crawlers for Slashdot, Digg and LastFM.



The idea is the following:

   - Clients are as stateless as possible.

     They periodically contact a server, request some work, perform any
     action needed to complet that job and submit the result back to the
     server. That's it. No local storage. No state saved between
     requests.

     For instance, a server my request a client to download and news
     article; if this is a multi-page article, the client will download
     the first page, see how many pages this given article has,
     concatenate them all and return them compressed to the server.

   - Servers are constructed to be fault-tolerant. We know they will
     crash, so every job result returned from a client is stored into
     stable storage.

   
     Well... I should probably describe more how servers are organized
     and stuff but, for know, this will be marked as TBD.



License
=======

Copying and license issues are discussed in LICENSE.txt

