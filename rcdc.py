import copy
import click
import random
import logbook
import sys
import itertools
import heapq

logbook.StreamHandler(sys.stdout).push_application()
logger = logbook.Logger('Switch')

# TODO:
# Exponential decay instead of multiple queues
# Many EPs per tick

# Compose / Concatenate / Merge distributions
# Add distributions: Single,
# Improve logging (more fine-grained; support logging each tick)

class Q(object):
    def __init__(self):
        self._l = []
        self._curr = 0

    def __next__(self):
        if self._curr == len(self._l):
            self._curr = 0
            return None
        
        next = self._l[self._curr]
        self._curr += 1
        return next
         
class LRU(Q):
    def __init__(self, n):
        Q.__init__(self)
        self._n = n;
        
    def enqueue(self, n):
        if n in self._l:
            self._l = [n] + [i for i in self._l if i != n]
            return
        if len(self._l) >= self._n:
           self._l.pop(0)
        self._l.append(n)

    def get(self):
        return self._l
    
    def __repr__(self):
        _EP_NUM_LEN = 6
        r = "     +"
        for i in range(self._n):
            r += "-" * _EP_NUM_LEN + "+"
        r += "\n"
        r += " --> "
        r += "|"
        for i in self._l[::-1]:
            r += str(i).center(_EP_NUM_LEN) + "|"
        r += " -->\n"
        r += "     +"
        for i in range(self._n):
            r += "-" * _EP_NUM_LEN + "+"
        return r


class EP:
    def __init__(self, id):
        self._id = id
        self._hits = 0
        
    def increment(self):
        self._hits += 1
    
    def __repr__(self):
        return "(%s,%s)" % (self._id, self._hits)

    def __lt__(self, other):
        return self._hits < other._hits


class CountQ(Q):
    def __init__(self):
        Q.__init__(self)
        
    def _increment_ep(self, id):
        for ep in self._l:
            if id == ep._id:
                break
        else:
            ep = EP(id)
            self._l.append(ep)

        ep.increment()

    def aggregate(self, latest):
        id = next(latest)
        while id != None:
            self._increment_ep(id)
            id = next(latest)
                
    def reset(self):
        self.last = self._l
        Q.__init__(self)
                  
class Sim(object):
    def __init__(self, queue_length, window_size, rc_thresh, rc_avail):
        self._window_size = window_size
        self._rc_thresh = rc_thresh
        self._rc_avail = rc_avail
        self._rc_eps = []
        self._dc_rc_switches = 0
        self.latest = LRU(queue_length)
        self.aggregated = CountQ()

    def __repr__(self):
        r = "EP num (msgs sent)\n"
        r += "RC Endpoints: { "
        for ep in sorted(self._rc_eps, reverse=True):
            r += str(ep) + ", "
        r += " }\n"
        r += "DC Endpoints: { "
        for ep in sorted(self.aggregated.last, reverse=True):
            if ep._id not in [ep._id for ep in self._rc_eps]:
                r += str(ep) + ", "
        r += " }\n"
        r += "\n"
        r += "DC -> RC switches: " + str(self._dc_rc_switches) + "\n"
        return r
    
    
    def add(self, ep_number):
        logger.trace(f"completed EP {ep_number}")
        self.latest.enqueue(ep_number)
        
    
    def tick(self):
        self.aggregated.aggregate(self.latest)

    def get_id(self, ep):
        return ep._id
    
    def _get_important_eps(self):
        important_eps = []
        
        ep = next(self.aggregated)
        while ep != None:
            if ep._hits >= self._rc_thresh * self._window_size:
                important_eps.append(ep)
            ep = next(self.aggregated)
        
        return important_eps
    
    def flush(self):
        important_eps = self._get_important_eps()
        self._rc_eps = [ep for ep in self.aggregated._l if ep._id in [ep._id for ep in self._rc_eps]]
        heapq.heapify(self._rc_eps)
        
        for ep in important_eps:
            if ep._id in [ep._id for ep in self._rc_eps]:
                continue
            
            if len(self._rc_eps) < self._rc_avail:
                heapq.heappush(self._rc_eps, ep)
            elif ep._hits - self._rc_eps[0]._hits > self._window_size * 0.05:
                logger.debug('replace %s with %s' % (self._rc_eps[0],ep))
                self._dc_rc_switches += 1
                heapq.heappushpop(self._rc_eps, ep)
        
        self.aggregated.reset()
        logger.debug('rc eps: %s' % sorted(self._rc_eps, reverse=True, key=self.get_id))
        
class Distribution(object):
    def __next__(self):
        raise NotImplementedError

class Uniform(Distribution):
    def __init__(self, n):
        self._n = n

    def __next__(self):
        return random.randint(0, self._n - 1)

class Gaussian(Distribution):
    def __init__(self, n):
        self._n = n

    def __next__(self):
        return max(min(int(random.gauss(self._n / 2, self._n / 20)), self._n - 1), 0)
    
class RoundRobin(Distribution):
    def __init__(self, n):
        self._n = n
        self._i = 0

    def __next__(self):
        t =  self._i % self._n
        self._i += 1
        return t

@click.command()
@click.option('-n', "--queue-length", default=20, help="Length of the queue")
@click.option('-w', "--window-size", default=500, help="Number of times we sample EPs before recalculate RC list")
@click.option('-e', "--endpoints", default=100, help="Number of endpoints")
@click.option('-t', "--ticks", default=10000, help="Number of ticks")
@click.option('-r', "--rc-thresh", default=0.4, help="RC threshold")
@click.option('-a', "--rc-avail", default=16, help="RC resources available")
@click.option('-p', "--packets-per-tick", default=20, help="How many packets are transmitted each tick")
@click.option('-l', "--log-level", default="INFO", help="Log level")
@click.option('-d', "--distribution", default="Uniform", help="Distribution name (choose from: Uniform, Gaussian, RoundRobin)")
def main(queue_length, window_size, endpoints, ticks, rc_thresh, rc_avail, packets_per_tick, distribution, log_level):
    log_level = logbook.lookup_level(log_level)
    logger.level = log_level

    logger.info(f"Starting simulation with:\n"
                f"Queue length: {queue_length}\n"
                f"Window size: {window_size}\n"
                f"Endpoints: {endpoints}\n"
                f"Ticks: {ticks}\n"
                f"RC threshold: {rc_thresh}\n"
                f"RC resources available: {rc_avail}\n"
                f"Distribution: {distribution}\n"
                f"Log level: {log_level}")
    logger.info(f"An endpoint can appear up to {window_size} times in the queues\n"
                f"On average it will appear {window_size/endpoints} times\n"
                f"To be important it needs to appear at least {rc_thresh*window_size} times\n")
    sim = Sim(queue_length, window_size, rc_thresh, rc_avail)
    distribution = globals()[distribution](endpoints)
    for t in range(ticks):

        for i in range(0,packets_per_tick):
            ep = next(distribution)
            sim.add(ep)
        
        sim.tick()
        
        if t > 0 and (t % window_size) == 0:
            sim.flush()

    logger.info(f"Finished simulation")
    logger.info(f"Simulator state:\n{sim}")

if __name__ == "__main__":
    main()