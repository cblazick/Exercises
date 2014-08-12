#! /usr/bin/python

import sys
import threading
import Queue
import urllib2
import optparse
import json
import time

ENTRIES_PER_PAGE = 50
RT_API_KEY = "skdrr2udj575jebmn7x68dud"
RT_URL = "http://api.rottentomatoes.com/api/public/v1.0/lists/movies/in_theaters.json?page=%d&page_limit=" + str(ENTRIES_PER_PAGE) + "&apikey=" + RT_API_KEY
PARALLEL_THREADS = 200 # 40 seems to be the end of most of the performance, 200 will probably
                       # leave some threads unused, but every scrap of performance will be used
MIN_QUEUE_SIZE = 150
DEBUG = None

# class helper functions
def retrieveContents(url):
    """
    get the contents of the webpage at a given url
    """
    
    while True:
        try:
            response = urllib2.urlopen(url, None, 2)
            contents = response.read()
            response.close()
            break
        except:
            continue
    
    return contents

def getData(url, contents=None):
    """
    retrieve the contents of a JSON page
    and convert it to a python element
    """
    
    if contents is None:
        contents = retrieveContents(url)
    data = json.loads(contents)
    
    return data

    
# classes and helper classes
class InvalidCommandException(Exception):
    pass

class IMDBException(Exception):
    pass
    
class DroneArmy:
    def __init__(self, numberOfThreads=PARALLEL_THREADS):
        """
        initiaizer generates desired drones and prepares to process commands
        """
        
        # initialize variables
        self.numberOfThreads = numberOfThreads
        self.stackSize = self.numberOfThreads*2+10
        if self.stackSize < MIN_QUEUE_SIZE:
            self.stackSize = MIN_QUEUE_SIZE
        self.queue = Queue.Queue(self.stackSize)
        self.totalEntries = None
        self.processedMovies = []
        self.results = []
        
        # spawn worker threads
        self.threads = []
        for i in range(self.numberOfThreads):
            t = threading.Thread(target=self.workerWaitLoop)
            t.daemon = True
            t.start()
            if DEBUG:
                print >> sys.stderr, "drone", i, "spawned"
            self.threads.append(t)
    
    def issueOrders(self, command):
        """ 
        used to add commands to the processing queue
        usually only "page:" commands are needed as further 
        commands get added by the page command handler
        """
        
        try:
            self.queue.put(command, False)
        except Queue.Full:
            print >> sys.stderr, "ERROR: queue full, raise min queue size in the globals of this script"
        
        return True
    
    def workerWaitLoop(self):
        """
        idle loop for the worker drone. calls helper
        functions when command is issued
        """
        
        # loops and waits to receive a command on the queue. garbage collection end the loop
        while True:
            try:
                task = self.queue.get(True)
                if DEBUG:
                    print >> sys.stderr, repr(task) + "\n",
                
                # hand the command off to the correct handler
                if task.startswith("page:"):
                    self.pageHandler(task)
                elif task.startswith("search:"):
                    self.searchHandler(task)
                elif task.startswith("count:"):
                    self.countHandler(task)
                else:
                    raise InvalidCommandException, "invalid command: " + repr(task)
                
                self.queue.task_done()
            
            except Exception:
                raise
            except:
                # (most likely raised during interpreter shutdown)
                pass
    
    def pageHandler(self, task):
        """
        processes RT page read commands
        """
        
        page = int(task.split(":", 1)[1])
        url = RT_URL % (page)
        
        # short circuits in case this is an extra page
        if self.totalEntries is not None and (page-1)*ENTRIES_PER_PAGE >= self.totalEntries:
            if DEBUG:
                print >> sys.stderr, "short circuit 1: page", page
            return
        
        # keep trying the page if there's an error until it clears ("too many requests" error)
        while True:
            data = getData(url)
            if not data.has_key("error"):
                break
        try:
            self.totalEntries = int(data["total"])
        except Exception, e:
			print >> sys.stderr, "ERR: " + str(data) + ", " + str(e) + "\n",
			return
        
        # second chance to short circuit now that TOTAL_ENRIES is guaranteed set
        if (page-1)*ENTRIES_PER_PAGE >= self.totalEntries:
            if DEBUG:
                print >> sys.stderr, "short circuit 2: page", page
            return
        
        # for each movies generate search(->count) or count jobs
        for movie in data["movies"]:
            rval = [movie["id"], movie["title"]]
            try:
                rval.append(movie["alternate_ids"]["imdb"])
                self.issueOrders("count:" + str(rval))
            except KeyError:
                rval.append(None)
                self.issueOrders("search:" + str(rval))
    
    def searchHandler(self, task):
        """
        processes IMDB title search commands
        """
        
        (id, title, imdbID) = eval(task.split(":", 1)[1])
        
        # short circuit if this is a duplicate
        if id in self.processedMovies:
            return
        
        pieces = title.split()
        # loop to search with progressively fewer words in case substitutions are present (i.e. And instead of &)
        while True:
            searchTerm = "%20".join(pieces)
            url = "http://www.omdbapi.com/?t=%s" % (searchTerm)
            if DEBUG:
                print >> sys.stderr, "search url:", url
            results = getData(url)
            try:
                imdbID = results["imdbID"].lstrip("t")
                break
            except KeyError:
                if DEBUG:
                    print >> sys.stderr, "ERR:", results
                del(pieces[-1])
            
            if len(pieces) == 0:
                msg = "error: couldn't find \"%s\" on IMDB" % (title)
                raise IMDBException, msg

        self.issueOrders("count:" + str([id, title, imdbID]))
    
    def countHandler(self, task):
        """
        processes IMDB img tag count commands
        """
        
        (id, title, imdbID) = eval(task.split(":", 1)[1])
        
        # short circuit if this is a duplicate
        if id in self.processedMovies:
            return
        else:
            self.processedMovies.append(id)
        
        url =  "http://www.imdb.com/title/tt" + imdbID
        contents = retrieveContents(url)

        # count the img tags
        imgCount = contents.count("<img ")
        
        # push back the data to be accumulated
        rval = {"url":url, "count":imgCount, "imdb_id":imdbID}
        rval = json.dumps(rval, indent=3)
        # do some maintainence to the string to make it match the example exactly
        rval = " " + rval.replace("\n", "\n ")
        self.results.append(rval)
    
    def waitForCompletion(self):
        """
        block until all commands have been performed
        """
        
        self.queue.join()
    
    def returnJSONOutput(self):
        """
        returns a string with a nicely formatted JSON
        object containing the results from the commands
        """
        
        rval = "[\n"
        rval += ",\n".join(self.results)
        rval += "\n]"
        
        return rval

def main(opts, args):
    if opts.threads:
        d = DroneArmy(int(opts.threads))
    else:
        d = DroneArmy()
    
    # order the page reads to get the initial movie list
    page = 1
    while True:
        if d.totalEntries is not None and DEBUG:
            print >> sys.stderr, "totalEntries:", d.totalEntries
        
        # if we have gotten the total, test it
        if d.totalEntries is not None and (page-1)*ENTRIES_PER_PAGE >= d.totalEntries:
            break
        
        d.issueOrders("page:" + str(page))
        time.sleep(0.25) # snooze to prevent too many page reads to generate before cleanup
        
        page += 1
        
    # wait for everything to complete
    d.waitForCompletion()
    
    # print the resulting JSON output
    print d.returnJSONOutput()
    
    if DEBUG:
        print >> sys.stderr, "RT totalEntries:", d.totalEntries
        print >> sys.stderr, "movie entries:", len(d.results)

if __name__ == "__main__":
    usage = "%prog"
    desc = "retrieve a list of movies in theaters from Rotten Tomatoes and count the imgs for them on their IMDB page"
    ver = "%prog v0.6"
    p = optparse.OptionParser(usage=usage, description=desc, version=ver)
    p.add_option("--debug", dest="debug", action="store_true", default=False, help=optparse.SUPPRESS_HELP)
    p.add_option("--threads", "-t", dest="threads", action="store", default=False, help="controls how many parallel threads are spawned")
    (opts, args) = p.parse_args()
    
    DEBUG = opts.debug

    main(opts, args)
