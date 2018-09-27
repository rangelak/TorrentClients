#!/usr/bin/python

# This is an implementation for the Standard BitTorrent Client
# Implemented by Rangel (Milushev) and Pancho (Francisco Trujillo)
# For the 2018 edition of CS136 at Harvard University

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer

class RanchoStd(Peer):
    def post_init(self):
        self.upload_slots = 4
        self.optimistic_slots = 1
        self.optimistically_unchoked_peer_id_list = [None]

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
        unchoked_peer_id_list = []

        current_round = history.current_round()

        cooperative_peers = {}

        if current_round > 1:
            # Since decisions are made every 10 secs, 20 seconds is best represented by two rounds.
            cooperative_peers = {d.from_id: d.blocks for d in history.downloads[current_round - 1]}

            for download in history.downloads[current_round - 2]:
                if download.from_id in cooperative_peers:
                    cooperative_peers[download.from_id] += download.blocks
                else:
                    cooperative_peers[download.from_id] = download.blocks

        # Nobody wants our pieces
        if len(incoming_requests) == 0:
            bandwidths = []
        else:
            requester_id_list = map(lambda req: req.requester_id, incoming_requests)

            # Requesters shuffled for impartiality
            random.shuffle(requester_id_list)

            cooperative_peer_id_list = map(lambda x: x[1], sorted(cooperative_peers.iteritems(), key=lambda (k,v): (v,k), reverse=True))

            # Keeps the order of the coopereative peers from most to least cooperative.
            cooperative_requester_id_list = filter(lambda cp: cp in requester_id_list, cooperative_peer_id_list)

            # Number of slots not usually optimistically unchoked
            reciprocative_slots = self.upload_slots - self.optimistic_slots

            # Add at most 3 peers by download speed ranking
            unchoked_peer_id_list = cooperative_requester_id_list[:reciprocative_slots]

            # Use the rest of the reciprocative slots to unchoke optimistically
            for requester_id in requester_id_list:
                if len(unchoked_peer_id_list) >= reciprocative_slots:
                    break
                elif requester_id not in unchoked_peer_id_list:
                    unchoked_peer_id_list.append(requester_id)

            # If the optimistically unchoked peer is not requesting any longer, replace it.
            if current_round % 3 == 0 or self.optimistically_unchoked_peer_id_list[0] not in requester_id_list:
                # Optimistically unchoke a peer
                for requester_id in requester_id_list:
                    if requester_id not in unchoked_peer_id_list:
                        unchoked_peer_id_list.append(requester_id)
                        self.optimistically_unchoked_peer_id_list = [requester_id]
                        break
            else:
                # Unchoke the same agent as in the previous round
                unchoked_peer_id_list.append(self.optimistically_unchoked_peer_id_list[0])

            # Evenly "split" my upload bandwidth among the unchoked requesters
            bandwidths = even_split(self.up_bw, len(unchoked_peer_id_list))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(unchoked_peer_id_list, bandwidths)]

        return uploads
