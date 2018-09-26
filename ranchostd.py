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

    def requests(self, peers, history):
        """
        peers: List of Peer objects.
        history: AgentHistory object.
        returns: List of Request objects.
        requests be called after update_pieces
        """
        lacks_blocks = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_piece_id_list = filter(lacks_blocks, range(len(self.pieces)))

        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_piece_id_list))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug("look at the AgentHistory class in history.py for details")
        logging.debug(str(history))

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
        for count, piece_id in sorted(pieces_by_holder_dict):

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
        peers: list of Peer objects.
        history: AgentHistory object.
        returns: list of Upload objects.
        uploads will be called after requests
        """
        unchoked_peer_id_list = []

        current_round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, current_round))

        cooperative_peer_id_list = []

        if current_round > 0:
            # Assuming the last round is ok instead of 20 secs.
            # Other clients might accumulate history of several rounds.
            recent_download_history = history.downloads[current_round - 1]
            cooperative_peer_id_list = [(d.blocks, d.from_id) for d in recent_download_history]

        if len(incoming_requests) == 0:
            logging.debug("No one wants my pieces!")
            bandwidths = []
        else:
            logging.debug("Unchoking peers.")

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
