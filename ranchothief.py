#!/usr/bin/python

# This is an implementation for the Standard BitTorrent Client
# Implemented by Rangel (Milushev) and Pancho (Francisco Trujillo)
# For the 2018 edition of CS136 at Harvard University

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer

class RanchoThief(Peer):
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
        sent_requests = []

        needed_pieces = self.needed_pieces_list()

        random.shuffle(needed_pieces)

        random.shuffle(peers)

        # Order the pieces by rarest first
        # [(piece_id, [holder_id_list])]
        pieces_by_holder_id_list = []
        for piece_id in needed_pieces:
            holder_peer_id_list = []

            for peer in peers:
                if piece_id in peer.available_pieces:
                    holder_peer_id_list.append(peer.id)

            # Add the pieces to the list and its holders
            if len(holder_peer_id_list) > 0:
                pieces_by_holder_id_list.append((piece_id, holder_peer_id_list))

        # Sort pieces by rarity
        # Tie breaking the sorting by prioritizing pieces that we're close to completing.
        # This is important to that we can start sharing them as soon as possible.
        pieces_by_rarity_list = sorted(pieces_by_holder_id_list, key=lambda (piece_id, holders): (len(holders), self.conf.blocks_per_piece - self.pieces[piece_id]))

        # Keep track of sent requests to not reach the max
        sent_requests_per_peer = {peer.id: 0 for peer in peers}

        # Requesting the rarest piece first
        for piece_id, holder_id_list in pieces_by_rarity_list:
            for holder_id in holder_id_list:
                # Don't make more requests than the maximum number of requests
                if sent_requests_per_peer[holder_id] < self.max_requests:
                    first_block = self.pieces[piece_id]
                    request = Request(self.id, holder_id, piece_id, first_block)
                    sent_requests.append(request)
                    sent_requests_per_peer[holder_id] += 1

        return sent_requests

    def uploads(self, incoming_requests, peers, history):
        return []

    def needed_pieces_list(self):
        return filter(lambda i: self.pieces[i] < self.conf.blocks_per_piece, range(len(self.pieces)))
