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
        self.assumed_peer_blocks = 4
        # {peer_id : flow_in_blocks}
        self.expected_download_from_peer_flow = dict()
        # {peer_id : (last_unchoked, flow_in_blocks)}
        # last_unchoked = 0 means the peer is choking us
        # 1,2,3 means we've been unchoked for x rounds.
        self.estimated_min_upload_rate_to_peer = dict()
        # {peer_id : (last_updated, [available_pieces])}
        self.recent_history_pieces_by_peer = dict()
        self.currently_unchoked_by_peer_set = set()
        self.unchoked_peer_set = set()
        self.bandwith_increasing_factor = 1.2
        self.bandwith_decreasing_factor = 0.9
        self.confidence_unchoked_periods = 3
        self.initial_min_upload_rate = 1

    def requests(self, peers, history):
        """
        peers: List of PeerInfo objects.
        history: AgentHistory object.
        returns: List of Request objects.
        requests be called after update_pieces
        """
        self.maintain_peer_data(peers, history)

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

    def uploads(self, incoming_requests, peers, history):
        """
        incoming_requests: list Request objects.
        peers: list of PeerInfo objects.
        history: AgentHistory object.
        returns: list of Upload objects.
        uploads will be called after requests
        """
        current_round = history.current_round()

        requester_id_list = []

        if len(incoming_requests) > 0:
            requester_id_list = map(lambda req: req.requester_id, incoming_requests)

            # Sorts from largest to smallest ratio
            sorted_requester_id_list = sorted(map(self.calculate_ratio, requester_id_list), reverse=True)

            # Preserves order but uses all the bandwidth
            # total_needed_bandwith = 0
            # for pid in requester_id_list:
            #     total_needed_bandwith += self.estimated_min_upload_rate_to_peer[pid][1]
            #     if total_needed_bandwith > self.up_bw:
            #         break

            # if total_needed_bandwith < self.up_bw:
            #     for pid in requester_id_list:
            #         self.estimated_min_upload_rate_to_peer[pid] = self.estimated_min_upload_rate_to_peer[pid][0], int(self.estimated_min_upload_rate_to_peer[pid][1] * self.up_bw / total_needed_bandwith)
            # else:
            bandwidth_accumulator = 0
            for index, pid in enumerate(requester_id_list):
                bandwidth_accumulator += self.estimated_min_upload_rate_to_peer[pid][1]
                if bandwidth_accumulator > self.up_bw:
                    # Dont include this one or the rest
                    requester_id_list = requester_id_list[:(index-1)]
                    break

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, pid, self.estimated_min_upload_rate_to_peer[pid][1]) for pid in requester_id_list]
        print uploads
        return uploads

    def maintain_peer_data(self, peers, history):
        current_round = history.current_round()

        # Initializing all the data
        if current_round == 0:
            initial_expected_min_upload_rate = self.up_bw / len(peers)
            self.recent_history_pieces_by_peer = {peer.id: (0, peer.available_pieces) for peer in peers}
            self.expected_download_from_peer_flow = {peer.id: 0 for peer in peers}
            self.estimated_min_upload_rate_to_peer = {peer.id: (0, initial_expected_min_upload_rate) for peer in peers}
            return

        last_round_download_history = history.downloads[current_round - 1]
        last_round_upload_history = history.uploads[current_round - 1]

        # Observed download flow
        known_capacity_peer_ids = set()
        for download in last_round_download_history:
            known_capacity_peer_ids.add(download.from_id)
            # This peer is unchoking us
            self.currently_unchoked_by_peer_set.add(download.from_id)
            self.expected_download_from_peer_flow[download.from_id] = download.blocks
            # We just downloaded something - means this peer is unchoking us
            if self.estimated_min_upload_rate_to_peer[download.from_id][0] < self.confidence_unchoked_periods:
                # Increment the rounds unchoked
                self.estimated_min_upload_rate_to_peer[download.from_id] = self.estimated_min_upload_rate_to_peer[download.from_id][0] + 1, self.estimated_min_upload_rate_to_peer[download.from_id][1]
            else:
                # Be more selfish with generous peers
                self.estimated_min_upload_rate_to_peer[download.from_id] = self.estimated_min_upload_rate_to_peer[download.from_id][0], self.estimated_min_upload_rate_to_peer[download.from_id][1] * self.bandwith_decreasing_factor

        # See if we should increase the expected upload rate
        for unchoked_peer in self.unchoked_peer_set:
            if unchoked_peer.id not in self.currently_unchoked_by_peer_set:
                self.estimated_min_upload_rate_to_peer[unchoked_peer.id] = 0, self.estimated_min_upload_rate_to_peer[unchoked_peer.id][1] * self.bandwith_increasing_factor

        # Estimated download flow for a single peer
        def estimate_flow(peer):
            if peer.id not in known_capacity_peer_ids:
                # This peer just choked us
                if peer.id in self.currently_unchoked_by_peer_set:
                    self.currently_unchoked_by_peer_set.remove(peer.id)
                pieces_before = len(self.recent_history_pieces_by_peer[peer.id][1])
                pieces_now = len(peer.available_pieces)
                last_updated = self.recent_history_pieces_by_peer[peer.id][0]
                # We will only update on change, else we'll keep our previous estimate
                if pieces_now != pieces_before:
                    # Estimating their rate based on how much it took them to complete a piece
                    self.expected_download_from_peer_flow[peer.id] = self.conf.blocks_per_piece * (pieces_now - pieces_before) / (current_round - last_updated) / self.assumed_peer_blocks
                    # Maintaining our records
                    self.recent_history_pieces_by_peer[peer.id] = (current_round, peer.available_pieces)

        map(estimate_flow, peers)

    def calculate_ratio(self, peer_id):
        if self.estimated_min_upload_rate_to_peer[peer_id][1] == 0:
            return float(inf), peer_id
        ratio = float(self.expected_download_from_peer_flow[peer_id]) / self.estimated_min_upload_rate_to_peer[peer_id][1]
        return ratio, peer_id


