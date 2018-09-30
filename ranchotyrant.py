#!/usr/bin/python

# This is an implementation for the Tyrant BitTorrent Client with no max capacity.
# Implemented by Rangel (Milushev) and Pancho (Francisco Trujillo)
# For the 2018 edition of CS136 at Harvard University

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer

class RanchoTyrant(Peer):
    def post_init(self):
        self.assumed_peer_slots = 4
        # {peer_id : flow_in_blocks}
        self.expected_peer_download_rate = dict()
        # {peer_id : flow_in_blocks}
        self.estimated_min_upload_rate_to_peer = dict()
        # {peer_id : rounds_ago}
        self.rounds_unchoked_by_peer = dict()
        # {peer_id : (rounds_ago, [available_pieces])}
        self.giver_peer_id_set = set()
        self.receiver_peer_id_set = set()
        self.bandwith_increasing_factor = 1.2
        self.bandwith_decreasing_factor = 0.9
        self.confidence_unchoked_periods = 2
        self.initial_min_upload_rate = self.up_bw / (self.assumed_peer_slots)

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
        current_round = history.current_round()

        # Initializing data
        if current_round == 0:
            self.expected_peer_download_rate = {peer.id: 0 for peer in peers}
            self.rounds_unchoked_by_peer = {peer.id: 0 for peer in peers}
            self.estimated_min_upload_rate_to_peer = {peer.id: self.initial_min_upload_rate for peer in peers}
        else:
            last_round_download_history = history.downloads[current_round - 1]
            last_round_upload_history = history.uploads[current_round - 1]

            self.receiver_peer_id_set = set(map(lambda upload: upload.to_id, last_round_upload_history))
            self.giver_peer_id_set = set(map(lambda download: download.from_id, last_round_download_history))

            # Adjusting upload rates
            for receiver_peer_id in self.receiver_peer_id_set:
                # If they are choking us
                if receiver_peer_id not in self.giver_peer_id_set:
                    # Increase min upload speed
                    self.estimated_min_upload_rate_to_peer[receiver_peer_id] *= self.bandwith_increasing_factor
                # If they're unchoking us
                else:
                    # Increase count of unchoked rounds
                    self.rounds_unchoked_by_peer[receiver_peer_id] += 1
                    if self.rounds_unchoked_by_peer[receiver_peer_id] >= self.confidence_unchoked_periods:
                        # Decrease min upload speed
                        self.estimated_min_upload_rate_to_peer[receiver_peer_id] *= self.bandwith_decreasing_factor

            # Accumulating total downloads for each peer
            downloaded_from_peer = {peer_id: 0 for peer_id in self.giver_peer_id_set}
            for download in last_round_download_history:
                downloaded_from_peer[download.from_id] += download.blocks

            # Observed download flow
            for peer_id, blocks in downloaded_from_peer.items():
                self.expected_peer_download_rate[peer_id] = blocks

            # Estimated download flow for a single peer
            for peer in peers:
                # These are the peers who didn't upload to us
                if peer.id not in self.giver_peer_id_set:
                    peer_pieces_now = len(peer.available_pieces)

                    # If the peer has the same pieces, we shouldn't be interested in uploading to them
                    interest_in_peer = len(set(self.needed_pieces_list()) & set(peer.available_pieces))

                    if interest_in_peer > 0:
                        # Estimating their rate based on the game structure
                        self.expected_peer_download_rate[peer.id] = self.conf.blocks_per_piece * peer_pieces_now / current_round / self.assumed_peer_slots
                    else:
                        # We don't care about this peer
                        self.expected_peer_download_rate[peer.id] = 0

        sorted_requester_id_list = []
        used_bandwidths = []

        if len(incoming_requests) > 0:
            # We don't want duplicates
            requester_id_list = list({r.requester_id for r in incoming_requests})

            # Random order
            random.shuffle(requester_id_list)

            # Sorts from largest to smallest ratio
            sorted_requester_id_list = sorted(requester_id_list, key=lambda peer_id: self.peer_ratio(peer_id), reverse=True)

            # Using up the bandwith
            bandwidth_accumulator = 0
            for index, peer_id in enumerate(sorted_requester_id_list):
                bandwidth_accumulator += int(self.estimated_min_upload_rate_to_peer[peer_id])
                if bandwidth_accumulator > self.up_bw:
                    # Dont include this one or the rest
                    sorted_requester_id_list = sorted_requester_id_list[:index]
                    break

            used_bandwidths = map(lambda pid: int(self.estimated_min_upload_rate_to_peer[pid]), sorted_requester_id_list)

        # Create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, pid, bw) for pid, bw in zip(sorted_requester_id_list, used_bandwidths)]
        return uploads

    def peer_ratio(self, peer_id):
        ratio = float(self.expected_peer_download_rate[peer_id]) / (self.estimated_min_upload_rate_to_peer[peer_id] + 1)
        return ratio, peer_id

    def needed_pieces_list(self):
        return filter(lambda i: self.pieces[i] < self.conf.blocks_per_piece, range(len(self.pieces)))



