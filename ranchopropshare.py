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
        # {peer_id : flow_in_blocks}
        self.peer_download_rate = dict()
        # {peer_id : (rounds_ago, [available_pieces])}
        self.giver_peer_id_set = set()
        self.receiver_peer_id_set = set()
        self.optimistic_unchoking_bandwidth = 0.1
        self.reciprocative_bandwidth = 1 - self.optimistic_unchoking_bandwidth
        self.optimistically_unchoked_peer = None

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
        """
        incoming_requests: list Request objects.
        peers: list of PeerInfo objects.
        history: AgentHistory object.
        returns: list of Upload objects.
        uploads will be called after requests
        """
        current_round = history.current_round()

        # [(peer, bandwidth)]
        bandwidth_by_peer = []

        if current_round > 0:
            last_round_download_history = history.downloads[current_round - 1]
            last_round_upload_history = history.uploads[current_round - 1]

            self.receiver_peer_id_set = set(map(lambda upload: upload.to_id, last_round_upload_history))
            self.giver_peer_id_set = set(map(lambda download: download.from_id, last_round_download_history))

            # Accumulating total downloads for each peer
            downloaded_from_peer = {peer_id: 0 for peer_id in self.giver_peer_id_set}
            for download in last_round_download_history:
                downloaded_from_peer[download.from_id] += download.blocks

            self.peer_download_rate = dict()

            # Observed download flow
            for peer_id, blocks in downloaded_from_peer.items():
                self.peer_download_rate[peer_id] = blocks

        if len(incoming_requests) > 0:
            # We don't want duplicates
            requester_id_list = list({r.requester_id for r in incoming_requests})

            # Random order
            random.shuffle(requester_id_list)

            # Calculate the bandwidth percentage, based on what the others requested
            for requester_id in requester_id_list:
                if requester_id in self.giver_peer_id_set:
                    bandwidth_by_peer.append((requester_id, self.peer_download_rate[requester_id]))
                else:
                    self.optimistically_unchoked_peer = requester_id

            if len(self.giver_peer_id_set) > 0:
                total_download_volume = sum(map(lambda (pid, download): download, self.peer_download_rate.items()))

                bandwidth_by_peer = map(lambda (pid, blocks): (pid, int(self.reciprocative_bandwidth * self.up_bw * blocks / total_download_volume)), bandwidth_by_peer)

                remaining_bandwith = self.up_bw - sum(map(lambda (pid, bw): bw, bandwidth_by_peer))
                print remaining_bandwith

                bandwidth_by_peer.append((self.optimistically_unchoked_peer, remaining_bandwith))
            else:
                bandwidth_by_peer = zip(requester_id_list, even_split(self.up_bw, len(requester_id_list)))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in bandwidth_by_peer]
        return uploads

    def needed_pieces_list(self):
        return filter(lambda i: self.pieces[i] < self.conf.blocks_per_piece, range(len(self.pieces)))
