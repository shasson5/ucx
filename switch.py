import sys

class Message:
    def __init__(self, src, dest, score, type):
        self.src   = src
        self.dest  = dest
        self.score = score
        self.type  = type

class EP:
    def __init__(self, dest, score):
        self.dest = dest
        self.score = score
        
    def __repr__(self):  
        return '(%s, %s)' % (self.dest, self.score)
    
    def get_score(self):
        return self.score
    
class Node:
    def __init__(self, id, eps, pending, max_rc):
        self.dc_list = eps
        self.rc_list = []
        self.pending = pending
        self.id      = id
        self.max_rc  = max_rc

    def show(self):
        print('node: %s' % self.id)
        print('rc: %s' % self.rc_list)
        print('dc: %s' % self.dc_list)

    def log(self, msg):
        print 'Node %u --> %s' % (self.id, msg)
    
    def error(self, msg):
        print 'Node %d: Error: %s' % (self.id, msg)
        exit(1)
    
    def _is_rc(self, ep):
        return ep.dest in [rc.dest for rc in self.rc_list]
        
    def promote(self):
        all_eps = self.rc_list + self.dc_list
        all_eps.sort(reverse=True, key=EP.get_score)
        
        for candidate in all_eps[:self.max_rc]:
            if self._is_rc(candidate):
                continue
        
            self._promote(candidate)    
    
    def _promote(self, ep):
        self.log('sending promotion request to node %d' % ep.dest)
        self.pending.append(Message(self.id, ep.dest, ep.score, 'Promote'))
        
    def _find_ep(self, req, eps):
        for ep in eps:
            # Need to compare remote src with local dest to match EP.
            if req.src == ep.dest:
                return ep
        
        return None
        
    def _handle_demote_req(self, req):
        self.log('received demotion request from node %d' % req.src)
        ep = self._find_ep(req, self.rc_list)
        
        if ep == None:
            self.error('demotion request: EP %u not in RC list' % req.src)
        
        self.rc_list.remove(ep)
        self.dc_list.append(ep)
        
        next = max(self.dc_list, key=lambda x: x.score)
        self._promote(next)
            
    def _get_min_rc(self):
        min_rc  = self.rc_list[0]
        
        for ep in self.rc_list:
            if ep.score < min_rc.score:
                min_rc = ep
              
        return min_rc

    def _switch_to_rc(self, ep):
        self.dc_list.remove(ep)
        self.rc_list.append(ep)
        self.log('sending ack to node %s' % ep.dest)
        self.pending.append(Message(self.id, ep.dest, ep.score, 'Ack'))

    def _demote_last(self):
        min_rc = self._get_min_rc()
        self.rc_list.remove(min_rc)
        self.dc_list.append(min_rc)
        self.log('sending demotion request to node %d' % min_rc.dest)
        self.pending.append(Message(self.id, min_rc.dest, min_rc.score, 'Demote'))

    def _handle_promote_req(self, req):
        self.log('received promotion request from node %d' % req.src)
        
        ep = self._find_ep(req, self.dc_list)
        if ep == None:
            self.error('promotion request: EP %u not in DC list' % req.src)
        
        # Update score to be the max of tx/rx
        ep.score = max(ep.score, req.score)
        
        if self.rc_avail() > 0:
            self._switch_to_rc(ep)
            return
        
        min_rc = self._get_min_rc()
        
        if ep.score > min_rc.score:
            self._switch_to_rc(ep)
            self._demote_last()
        else:
            self.log('request denied: %s' % ep)
    
    #todo: refactor out common code from handle_ack and handle_promote_req
    
    def _handle_ack(self, msg):
        self.log('received ack from node %s' % msg.src)
        
        if self.rc_avail() == 0:            
            self._demote_last()

        ep = self._find_ep(msg, self.dc_list)
        if ep == None:
            self.error('Ack failed: EP %u not in DC list' % msg.src)

        # Update score to be the max of tx/rx
        ep.score = max(ep.score, msg.score)
        
        self.dc_list.remove(ep)
        self.rc_list.append(ep)
        
    def handle_req(self, msg):
        if msg.type == 'Promote':
            self._handle_promote_req(msg)
        elif msg.type == 'Demote':
            self._handle_demote_req(msg)
        elif msg.type == 'Ack':
            self._handle_ack(msg)
        else:
            self.error('unknown msg type: %s' % msg.type)
            
    def rc_avail(self):
        return max(self.max_rc - len(self.rc_list), 0)

               
class Graph:
    def __init__(self, nodes, pending):
        self.nodes   = nodes
        self.pending = pending
        
    def run(self):
        for node in self.nodes:
            node.promote()
            
            while len(self.pending) > 0:
                req  = self.pending.pop(0)
                node = self.nodes[req.dest]
                node.handle_req(req)
            
        print '\n\nResults:'
        print '=============='
        for node in self.nodes:
            node.show()


scenarios = []
pending   = []
max_rc    = 3

#todo: check convergance and correctness.

nodes = [Node(0, [EP(dest=1, score=100),EP(dest=2, score=110),EP(dest=3, score=120),EP(dest=4, score=130),EP(dest=5, score=140)], pending, max_rc),
         Node(1, [EP(dest=0, score=200),EP(dest=2, score=210),EP(dest=3, score=220),EP(dest=4, score=230),EP(dest=5, score=240)], pending, max_rc),
         Node(2, [EP(dest=0, score=300),EP(dest=1, score=310),EP(dest=3, score=320),EP(dest=4, score=330),EP(dest=5, score=340)], pending, max_rc),
         Node(3, [EP(dest=0, score=400),EP(dest=1, score=410),EP(dest=2, score=420),EP(dest=4, score=430),EP(dest=5, score=440)], pending, max_rc),
         Node(4, [EP(dest=0, score=500),EP(dest=1, score=510),EP(dest=2, score=520),EP(dest=3, score=530),EP(dest=5, score=540)], pending, max_rc),
         Node(5, [EP(dest=0, score=600),EP(dest=1, score=610),EP(dest=2, score=620),EP(dest=3, score=630),EP(dest=4, score=640)], pending, max_rc)]

scenarios.append(nodes)

for s in scenarios:
    g = Graph(s, pending)  
    g.run()


            