from client import BaseClient, getUUID, log_backtrace, log_urllib2_exception

# import important modules and bring client.py main classes into this
# namespace -- saves time, and avoids DistributedCrawler.client.client imports

__all__ = ["client", "article_retriever", "uuid", "daemonize", 
        "BeautifulSoup", "upload_aux",
        # client.py internal symbols
        "BaseClient", "getUUID", "log_backtrace", "log_urllib2_exception"
        ]
