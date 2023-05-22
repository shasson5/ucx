import copy
import click
import random
import logbook
import sys
import itertools


logbook.StreamHandler(sys.stdout).push_application()
logger = logbook.Logger('Switch')

# TODO:
# Exponential decay instead of multiple queues
# Many EPs per tick

# Compose / Concatenate / Merge distributions
# Add distributions: Single,
# Improve logging (more fine-grained; support logging each tick)

class Q(object):
    def __init__(self, n):
        self._l = []
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


class Sim(object):
    def __init__(self, queue_length, queues_number, endpoints, rc_thresh, rc_avail):
        self._queues_number = queues_number
        self._queue_length = queue_length
        self._endpoints = endpoints
        self._rc_thresh = rc_thresh
        self._rc_avail = rc_avail
        self._queues = []
        self._rc_eps = set()
        self._dc_eps = set()
        self._important_eps = set()
        self._rc_dc_switches = 0
        self._dc_rc_switches = 0
        self._msg_by_ep = {}

    def update_queues(self, new_queue):
        if self._queues_number == len(self._queues):
            self._queues.pop(0)
        self._queues.append(new_queue)

    def _enqueue(self, n):
        if self._queues:
            queue = copy.deepcopy(self._queues[-1])
        else:
            queue = Q(self._queue_length)

        queue.enqueue(n)
        self.update_queues(queue)

    def __repr__(self):
        r = "EP num (msgs sent)\n"
        r += "RC Endpoints: { "
        for ep in self._rc_eps:
            r += str(ep) + " (" + str(self._msg_by_ep.get(ep, 0)) + "), "
        r += " }\n"
        r += "DC Endpoints: { "
        for ep in self._dc_eps:
            r += str(ep) + " (" + str(self._msg_by_ep.get(ep, 0)) + "), "
        r += " }\n"
        r += "\n"
        r += "DC -> RC switches: " + str(self._dc_rc_switches) + "\n"
        r += "RC -> DC switches: " + str(self._rc_dc_switches) + "\n"
        r += "\n"
        r += "     + New +\n\n"
        for (i, q) in enumerate(self._queues[::-1]):
            r += f"{len(self._queues)-i-1}\n{q}\n\n"
        r += "     + Old +\n"
        return r
    
    def add_packets(self, ep_number):
        logger.debug(f"completed EP {ep_number}")
        self._msg_by_ep[ep_number] = self._msg_by_ep.get(ep_number, 0) + 1
        self._enqueue(ep_number)
        
        if ep_number not in self._rc_eps and ep_number not in self._dc_eps:
            logger.debug(f"EP {ep_number} starts in DC")
            self._dc_eps.add(ep_number)
    
    def tick(self):
        for ep in range(0, self._endpoints):
            count = 0
            for q in self._queues:
                if ep in q.get():
                    count += 1
            if count >= self._rc_thresh * self._queues_number:
                self._important_eps.add(ep)

        for ep in self._important_eps:
            if ep in self._rc_eps:
                continue
            logger.debug(f"EP {ep} is important, trying to switch to RC...")
            if len(self._rc_eps) < self._rc_avail:
                logger.debug(f"EP {ep} DC -> RC")
                self._rc_eps.add(ep)
                self._dc_eps.remove(ep)
                self._dc_rc_switches += 1
                continue

            for rc_ep in self._rc_eps:
                if rc_ep not in self._important_eps:
                    logger.debug(f"EP {rc_ep} RC -> DC")
                    self._rc_eps.remove(rc_ep)
                    self._dc_eps.add(rc_ep)
                    self._rc_dc_switches += 1
                    break
            else:
                logger.debug(f"Couldn't find a non-important EP to kick out of RC")
                break
            logger.debug(f"EP {ep} DC -> RC")
            self._dc_rc_switches += 1
            self._rc_eps.add(ep)
            self._dc_eps.remove(ep)


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
@click.option('-k', "--queues-number", default=50, help="Number of queues")
@click.option('-e', "--endpoints", default=100, help="Number of endpoints")
@click.option('-t', "--ticks", default=2000, help="Number of ticks")
@click.option('-r', "--rc-thresh", default=0.1, help="RC threshold")
@click.option('-a', "--rc-avail", default=16, help="RC resources available")
@click.option('-l', "--log-level", default="INFO", help="Log level")
@click.option('-d', "--distribution", default="Uniform", help="Distribution name (choose from: Uniform, Gaussian, RoundRobin)")
def main(queue_length, queues_number, endpoints, ticks, rc_thresh, rc_avail, distribution, log_level):
    log_level = logbook.lookup_level(log_level)
    logger.level = log_level

    logger.info(f"Starting simulation with:\n"
                f"Queue length: {queue_length}\n"
                f"Queues number: {queues_number}\n"
                f"Endpoints: {endpoints}\n"
                f"Ticks: {ticks}\n"
                f"RC threshold: {rc_thresh}\n"
                f"RC resources available: {rc_avail}\n"
                f"Distribution: {distribution}\n"
                f"Log level: {log_level}")
    logger.info(f"An endpoint can appear up to {queues_number} times in the queues\n"
                f"On average it will appear {queues_number/endpoints} times\n"
                f"To be important it needs to appear at least {rc_thresh*queues_number} times\n")
    sim = Sim(queue_length, queues_number, endpoints, rc_thresh, rc_avail)
    distribution = globals()[distribution](endpoints)
    for t in range(ticks):
        ep = next(distribution)
        sim.add_packets(ep)
        sim.tick()

    logger.info(f"Finished simulation")
    logger.info(f"Simulator state:\n{sim}")

if __name__ == "__main__":
    main()