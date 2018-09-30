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
            requester_id_list = list({r.requester_id for r in incoming_requests})

            # Requesters shuffled for impartiality
            random.shuffle(requester_id_list)

            cooperative_peer_id_list = map(lambda x: x[1], sorted(cooperative_peers.iteritems(), key=lambda (k,v): (v,k), reverse=True))

            # Keeps the order of the cooperative peers from most to least cooperative.
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

    def needed_pieces_list(self):
        return filter(lambda i: self.pieces[i] < self.conf.blocks_per_piece, range(len(self.pieces)))
