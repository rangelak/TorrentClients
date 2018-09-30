#!/usr/bin/python

# This is an implementation of the RanchoTM BitTorrent Client
# Implemented by Rangel (Milushev) and Pancho (Francisco Trujillo)
# For the 2018 edition of CS136 at Harvard University

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer

class RanchoTourney(Peer):
    def post_init(self):
        self.upload_slots = 4
        self.optimistic_slots = 1

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

        # Finding which peers have the pieces we need
        # [(piece_id, [holder_id_list])]
        pieces_by_holder_id_list = []
        for piece_id in needed_pieces:
            holder_peer_id_list = []
            for peer in peers:
                if piece_id in peer.available_pieces:
                    holder_peer_id_list.append(peer.id)
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
        """
        incoming_requests: list Request objects.
        peers: list of PeerInfo objects.
        history: AgentHistory object.
        returns: list of Upload objects.
        uploads will be called after requests
        """
        unchoked_peer_id_list = []

        current_round = history.current_round()

        cooperative_peer_id_list = []

        if current_round > 0:
            # Assuming the last round is ok instead of 20 secs.
            # Other clients might accumulate history of several rounds.
            recent_download_history = history.downloads[current_round - 1]
            cooperative_peer_id_list = [(d.blocks, d.from_id) for d in recent_download_history]

        if len(incoming_requests) == 0:
            bandwidths = []
        else:
            requester_id_list = map(lambda req: req.requester_id, incoming_requests)

            # Requesters shuffled for impartiality
            random.shuffle(requester_id_list)

            cooperative_requester_id_list = sorted(filter(lambda cp: cp[1] in requester_id_list, cooperative_peer_id_list),reverse=True)

            # Add at most 3 peers by download speed ranking
            unchoked_peer_id_list = cooperative_requester_id_list[:(self.upload_slots - self.optimistic_slots)]

            # Use the rest of the slots to unchoke optimistically
            for requester_id in requester_id_list:
                if len(unchoked_peer_id_list) < 4 and requester_id not in unchoked_peer_id_list:
                    unchoked_peer_id_list.append(requester_id)

            # Evenly "split" my upload bandwidth among the unchoked requesters
            bandwidths = even_split(self.up_bw, len(unchoked_peer_id_list))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(unchoked_peer_id_list, bandwidths)]

        return uploads
