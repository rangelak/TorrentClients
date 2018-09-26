#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer


class rmftStd(Peer):
    # Post initialization, init instance variables.
    def post_init(self):
        pass

    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
            List of Peers.
        history: what's happened so far as far as this peer can see
            History, only visible (concerning) this peer, instance of AgentHistory class.
        returns: a list of Request() objects.

        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))
        np_set = set(needed_pieces)  # sets support fast intersection ops.


        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug("look at the AgentHistory class in history.py for details")
        logging.debug(str(history))

        # We'll put all the things we want here
        sent_requests = []

        # Symmetry breaking is good...
        random.shuffle(needed_pieces)
        
        # Sort peers by id.  This is probably not a useful sort, but other 
        # sorts might be usefulo first sort
        # Top contribut
        peers.sort(key=lambda p: p.id)
        # request all available pieces from all peers!
        
        # Order the pieces by rarest first
        ordered_pieces = dict()
        for piece in np_set:
            # Peers who have the piece
            holders = []
            
            for peer in peers:
                if piece in peer.available_pieces:
                    holders.append(peer.id)

            # Add the peers who have the piece to the dictionary
            ordered_pieces[(len(holders), piece)] = holders

        # Requesting the rarest piece first
        for count, piece_id in sorted(ordered_pieces):
            
            # Don't make more requests than the maximum number of requests
            if self.max_requests == len(sent_requests):
                break
            
            holders = ordered_pieces[(count, piece_id)]
            for holder in holders:
                start_block = self.pieces[piece_id]
                r = Request(self.id, holder, piece_id, start_block)
                sent_requests.append(r)

        return sent_requests
    
    """
    If peer i uploads with more than the third highest current download, unchoke i next period.
    Else if it uploads with less than the third highest download, choke in next period
    Optimistically unchoke random peer every third period.
    """  
    def uploads(self, incoming_requests, peers, history):
        """
        incoming_requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """
        # defining the number of upload slots
        # Define in class variables.  
        upload_slots = 4

        # the people we are choosing to upload to
        unchoked_peers = []

        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.

        # dictionary for nice, good-willed clients who let us download from them
        cooperative_clients = dict()

        # let's fill the dictionary
        if round >= 2:
            download_hist = history.downloads[round - 1] + history.downloads[round - 2]
            for download in download_hist:
                if download.from_id not in cooperative_clients:          
                    cooperative_clients[download.from_id] = download.blocks
                else:
                    cooperative_clients[download.from_id] += download.blocks

        if len(incoming_requests) == 0:
            logging.debug("No one wants my pieces!")
            bandwidths = []
        else:
            logging.debug("Still here: uploading to a random peer")
            
            # Unchoking before we have data or if there are less requesters than slots
            if round < 2 or len(incoming_requests) < upload_slots:
                for i in range(upload_slots):
                    
                    # Making sure that since we remove incoming_requests, the list isn't empty
                    if len(incoming_requests) != 0:
                        incoming_request = random.choice(incoming_requests)
                        # Getting rid of the request for the next iteration 
                        incoming_requests.remove(incoming_request)
                        unchoked_peers.append(incoming_request.requester_id)
                
            else:
                # Starting counter at 1 to keep one slot for optimistic unchoking 
                counter = 1 
                # TODO: Check this sort (inverted)
                for cooperative_client_id, value in sorted(cooperative_clients.iteritems(), key=lambda (k,v): (v,k)):
                    if counter == upload_slots:
                        break

                    if len(incoming_requests) != 0:
                        for incoming_request in incoming_requests:
                            if cooperative_client_id == incoming_request.requester_id:
                                unchoked_peers.append(cooperative_client_id)
                                del cooperative_clients[cooperative_client_id]
                                incoming_requests.remove(incoming_request)
                                counter += 1
                                break
                
                # TODO: not every single turn, check the peer is not already unchoked.
                # Use unchoked peers.
                # unchoke 1 peer optimistically
                if len(incoming_requests) != 0:
                    request = random.choice(incoming_requests)
                    unchoked_peers.append(request.requester_id)            
            # Evenly "split" my upload bandwidth among the unchoked requesters
            bandwidths = even_split(self.up_bw, len(unchoked_peers))


        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(unchoked_peers, bandwidths)]
            
        return uploads