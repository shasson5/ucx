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
        self.eps     = eps
        self.pending = pending
        self.id      = id
        self.rc_list = []
        self.max_rc  = max_rc
    
    def log(self, msg):
        print 'Node %u --> %s' % (self.id, msg)
    
    def error(self, msg):
        print 'Error: %s' % msg
        
    def promote(self, max = sys.maxint):
        all_eps = self.eps + self.rc_list
        all_eps.sort(reverse=True, key=EP.get_score)
        
        candidates = []
        for ep in all_eps:
            if ep.dest not in [rc.dest for rc in self.rc_list]:
                candidates.append(ep)
        
        # trancate max elements to process
        max = min(max, self.max_rc)
        
        for candidate in candidates[:max]:
            self.log('sending promotion request to node %d' % candidate.dest)
            self.pending.append(Message(self.id, candidate.dest, candidate.score, 'Promote'))
        
    def _find_in_rc_list(self, req):
        for ep in self.rc_list:
            # Need to compare remote src with local dest to match EP.
            if req.src == ep.dest:
                return ep
        
        return None
        
    def _handle_demote_req(self, req):
        self.log('received demotion request from node %d' % req.src)
        ep = self._find_in_rc_list(req)
        
        if ep == None:
            self.error('demotion requset: already DC -> bug')
            exit(1)
        
        self.rc_list.remove(ep)
        self.promote(1)

    def _get_min_rc(self):
        min_rc  = self.rc_list[0]
        
        for ep in self.rc_list:
            if ep.score < min_rc.score:
                min_rc = ep
              
        return min_rc

    def _add_rc(self, ep):
        self.rc_list.append(ep)
        self.log('sending ack to node %s' % ep.dest)
        self.pending.append(Message(self.id, ep.dest, ep.score, 'Ack'))

    def _remove_least_active_rc(self):
        min_rc = self._get_min_rc()
        self.rc_list.remove(min_rc)
        self.log('sending demotion request to node %d' % min_rc.dest)
        self.pending.append(Message(self.id, min_rc.dest, min_rc.score, 'Demote'))

    def _handle_promote_req(self, req):
        self.log('received promotion request from node %d' % req.src)
        
        ep = EP(req.src, req.score)
        if self.rc_avail() > 0:
            self._add_rc(ep)
            return
        
        min_rc = self._get_min_rc()
        
        if ep.score > min_rc.score:
            self._add_rc(ep)
            self._remove_least_active_rc()
        else:
            self.log('requset denied: %s' % ep)

    def _handle_ack(self, msg):
        self.log('received ack from node %s' % msg.src)
        
        if self.rc_avail() > 0:
            self.rc_list.append(EP(msg.src, msg.score))
        else:
            self._remove_least_active_rc()
     
    def handle_req(self, msg):
        if msg.type == 'Promote':
            self._handle_promote_req(msg)
        elif msg.type == 'Demote':
            self._handle_demote_req(msg)
        elif msg.type == 'Ack':
            self._handle_ack(msg)
        else:
            self.error('unknown msg type: %s' % msg.type)
            exit(1)
            
    def rc_avail(self):
        return self.max_rc - len(self.rc_list)

               
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
            print('node: %u' % node.id)
            print('rc: %s' % node.rc_list)
         

scenarios = []
pending   = []
max_rc    = 3

# nodes = [Node(0, [EP(dest=2, score=100)], pending, 3),
#          Node(1, [EP(dest=0, score=200)], pending, 3),
#          Node(2, [EP(dest=0, score=300),EP(dest=1, score=300)], pending, 3),
#         ]
#
# scenarios.append(nodes)

nodes = [Node(0, [EP(dest=4, score=100),EP(dest=1, score=120),EP(dest=3, score=150)], pending, max_rc),
         Node(1, [EP(dest=0, score=200),EP(dest=2, score=210)], pending, max_rc),
         Node(2, [EP(dest=1, score=310),EP(dest=0, score=300)], pending, max_rc),
         Node(3, [], pending, max_rc),
         Node(4, [], pending, max_rc)]

scenarios.append(nodes)

for s in scenarios:
    g = Graph(s, pending)  
    g.run()


            