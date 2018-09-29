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
        # {peer_id : (last_unchoked, flow_in_blocks)}
        # last_unchoked = 0 means the peer is choking us
        # 1,2,3 means we've been unchoked for x rounds.
        self.estimated_min_upload_rate_to_peer = dict()
        # {peer_id : (last_updated, [available_pieces])}
        self.recent_history_pieces_by_peer = dict()
        self.cooperative_peer_set = set()
        self.unchoked_peer_set = set()
        self.bandwith_increasing_factor = 1.2
        self.bandwith_decreasing_factor = 0.9
        self.confidence_unchoked_periods = 3
        self.initial_min_upload_rate = int(self.up_bw / (self.assumed_peer_slots + 1))

    def requests(self, peers, history):
        """
        peers: List of PeerInfo objects.
        history: AgentHistory object.
        returns: List of Request objects.
        requests be called after update_pieces
        """
        self.maintain_peer_data(peers, history)

        sent_requests = []

        needed_pieces = self.needed_pieces_list()

        random.shuffle(needed_pieces)

        random.shuffle(peers)

        # Order the pieces by rarest first
        # [(number_holders, piece_id, [holder_id_list])]
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

        requester_id_list = []
        used_bandwidths = []

        if len(incoming_requests) > 0:
            requester_id_list = list({r.requester_id for r in incoming_requests})

            random.shuffle(requester_id_list)

            # Sorts from largest to smallest ratio
            sorted_requester_id_list = sorted(map(self.calculate_ratio, requester_id_list), key=lambda (ratio, id): ratio, reverse=True)

            # Using up the bandwith
            bandwidth_accumulator = 0
            for index, pid in enumerate(requester_id_list):
                bandwidth_accumulator += self.estimated_min_upload_rate_to_peer[pid][1]
                if bandwidth_accumulator > self.up_bw:
                    # Dont include this one or the rest
                    requester_id_list = requester_id_list[:index]
                    break

            used_bandwidths = map(lambda pid: self.estimated_min_upload_rate_to_peer[pid][1], requester_id_list)

            # Use up all the bandwith that's left
            if bandwidth_accumulator < self.up_bw:
                used_bandwidths = map(lambda bw: int(bw * self.up_bw / bandwidth_accumulator), used_bandwidths)

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, pid, bw) for pid, bw in zip(requester_id_list, used_bandwidths)]
        return uploads

    def maintain_peer_data(self, peers, history):
        current_round = history.current_round()

        # Initializing all the data
        if current_round == 0:
            self.recent_history_pieces_by_peer = {peer.id: (0, peer.available_pieces) for peer in peers}
            self.expected_peer_download_rate = {peer.id: 0 for peer in peers}
            self.estimated_min_upload_rate_to_peer = {peer.id: (0, self.initial_min_upload_rate) for peer in peers}
            return

        # Round > 0
        last_round_download_history = history.downloads[current_round - 1]

        # Observed download flow
        for download in last_round_download_history:
            # This peer is unchoking us
            self.cooperative_peer_set.add(download.from_id)
            self.expected_peer_download_rate[download.from_id] = download.blocks
            # We just downloaded something - means this peer is unchoking us
            round_count, upload_rate = self.estimated_min_upload_rate_to_peer[download.from_id]
            if round_count < self.confidence_unchoked_periods:
                self.estimated_min_upload_rate_to_peer[download.from_id] = round_count + 1, upload_rate
            else:
                # Decrease upload speed
                self.estimated_min_upload_rate_to_peer[download.from_id] = round_count + 1, upload_rate * self.bandwith_decreasing_factor

        # Estimated download flow for a single peer
        for peer in peers:
            # These are the peers we did not just download from
            if peer.id not in self.cooperative_peer_set:
                peer_pieces_before = set(self.recent_history_pieces_by_peer[peer.id][1])
                peer_pieces_now = set(peer.available_pieces)
                last_updated = self.recent_history_pieces_by_peer[peer.id][0]

                # If the peer has the same pieces, we shouldn't be interested in uploading to them
                interest_in_peer = len(set(self.needed_pieces_list()) & peer_pieces_now)

                if interest_in_peer > 0:
                    # We will only update on change, else we'll keep our previous estimate
                    if peer_pieces_now != peer_pieces_before:
                        # Estimating their rate based on how much it took them to complete a piece
                        self.expected_peer_download_rate[peer.id] = self.conf.blocks_per_piece * (peer_pieces_now - peer_pieces_before) / (current_round - last_updated) / self.assumed_peer_slots
                        # Maintaining our records up to date
                        self.recent_history_pieces_by_peer[peer.id] = (current_round, peer.available_pieces)
                        # See if we should increase the expected upload rate to any of the peers we uploaded to
                        if peer in self.unchoked_peer_set:
                            self.estimated_min_upload_rate_to_peer[unchoked_peer.id] = 0, self.estimated_min_upload_rate_to_peer[unchoked_peer.id][1] * self.bandwith_increasing_factor
                else:
                    # We don't care about this peer
                    self.expected_peer_download_rate[peer.id] = 0


    def calculate_ratio(self, peer_id):
        ratio = float(self.expected_peer_download_rate[peer_id]) / self.estimated_min_upload_rate_to_peer[peer_id][1]
        return ratio, peer_id

    def needed_pieces_list(self):
        return filter(lambda i: self.pieces[i] < self.conf.blocks_per_piece, range(len(self.pieces)))



