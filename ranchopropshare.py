#!/usr/bin/python

# This is an implementation for the PropShare BitTorrent Client
# Implemented by Rangel (Milushev) and Pancho (Francisco Trujillo)
# For the 2018 edition of CS136 at Harvard University

import random
import logging
import math

from messages import Upload, Request
from util import even_split
from peer import Peer

class RanchoPropShare(Peer):
    def post_init(self):
        self.assumed_peer_blocks = 4
        self.optimistic_unchoking_bandwidth = 0.1
        self.optimistically_unchoked_peer = None

    def requests(self, peers, history):
        """
        peers: List of PeerInfo objects.
        history: AgentHistory object.
        returns: List of Request objects.
        requests be called after update_pieces
        """
        lacks_blocks = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_piece_id_list = filter(lacks_blocks, range(len(self.pieces)))

        sent_requests = []

        random.shuffle(needed_piece_id_list)

        random.shuffle(peers)

        # Order the pieces by rarest first
        pieces_by_holder_dict = dict()
        for piece_id in needed_piece_id_list:
            holder_peer_id_list = []

            for peer in peers:
                if piece_id in peer.available_pieces:
                    holder_peer_id_list.append(peer.id)

            # Add the peers who have the piece to the dictionary
            pieces_by_holder_dict[(len(holder_peer_id_list), piece_id)] = holder_peer_id_list

        # Requesting the rarest piece first
        for count, piece_id in sorted(pieces_by_holder_dict, key=lambda (k,v): k):

            # Don't make more requests than the maximum number of requests
            if self.max_requests == len(sent_requests):
                break

            holder_peer_id_list = pieces_by_holder_dict[(count, piece_id)]
            for holder in holder_peer_id_list:
                first_block = self.pieces[piece_id]
                r = Request(self.id, holder, piece_id, first_block)
                sent_requests.append(r)

        return sent_requests


    # If peer i uploads with more than the third highest current download, unchoke i next period.
    # Else if it uploads with less than the third highest download, choke in next period
    # Optimistically unchoke random peer every third period.

    def uploads(self, incoming_requests, peers, history):
        """
        incoming_requests: list Request objects.
        peers: list of PeerInfo objects.
        history: AgentHistory object.
        returns: list of Upload objects.
        uploads will be called after requests
        """
        current_round = history.current_round() 
        bandwidth_by_peer = []
        cooperative_peers = {}

        if current_round > 0:
            # (current_round - 1) is the last round. Get the cooperative peers from round t-1
            cooperative_peers = {d.from_id: d.blocks for d in history.downloads[current_round - 1]}

        # Nobody wants our pieces
        if len(incoming_requests) != 0:
            requester_id_list = map(lambda req: req.requester_id, incoming_requests)

            # Requesters shuffled for impartiality
            random.shuffle(requester_id_list)

            # Calculate the bandwidth percentage, based on what the others requested
            bandwidth_by_peer = self.allocate_bandwidth(cooperative_peers, requester_id_list)
            #print bandwidth_by_peer

            # If the optimistically unchoked peer is not requesting any longer, replace it.
            if current_round % 3 == 0 or self.optimistically_unchoked_peer not in requester_id_list:
                # Optimistically unchoke a peer
                for requester_id in requester_id_list:
                    if requester_id not in cooperative_peers:
                        self.optimistically_unchoked_peer = requester_id
                        optimistically_unchoked_tuple = (self.optimistically_unchoked_peer, self.optimistic_unchoking_bandwidth)
                        bandwidth_by_peer.append(optimistically_unchoked_tuple)
                        break

            bandwidth_by_peer = map(lambda (x, y): (x, math.floor(y*self.up_bw)), bandwidth_by_peer)
            #print bandwidth_by_peer



        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in bandwidth_by_peer]
        return uploads

    # function to allocate bandwidth by peer id, given that we use data for the cooperative peers from the last round
    def allocate_bandwidth(self, cooperative_peers, requester_id_list):
        bandwidth_by_peer = []
        
        # factor for the non-optimistic unchoking
        unchoking_factor = 1 - self.optimistic_unchoking_bandwidth
        
        if len(requester_id_list) == 0:
            return bandwidth_by_peer

        # iterate through the requesters and check if they cooperate before, if so, upload to them we shall!
        for requester_id in requester_id_list:
            if requester_id in cooperative_peers:
                download_from_requester = cooperative_peers[requester_id]
                bandwidth_by_peer.append((requester_id, download_from_requester))

        # get the total downloads from the peers you're interested in
        total_download_from_requesters = sum(d for p, d in bandwidth_by_peer)

        # Bandwidth per requester is proportional to the amount they let us download last round
        bandwidth_by_peer = map(lambda (x, y): (x, unchoking_factor*(y/float(total_download_from_requesters))), bandwidth_by_peer)
        return bandwidth_by_peer


