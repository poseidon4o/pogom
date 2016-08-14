#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import math
import time
import collections
import cProfile
import os
import json
import random
from datetime import datetime
from itertools import izip, count
from threading import Thread

from pgoapi import PGoApi
from pgoapi.utilities import f2i, get_cell_ids, get_pos_by_name
from sys import maxint
from geographiclib.geodesic import Geodesic

from .models import parse_map
from . import config

log = logging.getLogger(__name__)

def gSleep(val):
    steps = int(float(val) / 0.1)
    for i in range(0, steps):
        time.sleep(0.1)


class ScanMetrics:
    CONSECUTIVE_MAP_FAILS = 0
    STEPS_COMPLETED = 0
    NUM_STEPS = 1
    LOGGED_IN = 0.0
    LAST_SUCCESSFUL_REQUEST = 0.0
    COMPLETE_SCAN_TIME = 0
    NUM_THREADS = 0
    NUM_ACCOUNTS = 0
    CURRENT_SCAN_PERCENT = 0.0

# TODO: make thread count configurable
# TODO: make accounds scan adjacent cells to avoid big jumps
class Scanner(Thread):
    def __init__(self, scan_config):
        Thread.__init__(self)
        self.daemon = True
        self.name = 'search_thread'

        self.api = PGoApi(config['SIGNATURE_LIB_PATH'])
        self.scan_config = scan_config

    def next_position(self):
        for point in self.scan_config.COVER:
            yield (point["lat"], point["lng"], point['acc'])

    @staticmethod
    def callback(response_dict):
        if (not response_dict) or ('responses' in response_dict and not response_dict['responses']):
            log.info('Map Download failed. Trying again.')
            ScanMetrics.CONSECUTIVE_MAP_FAILS += 1
            return

        try:
            parse_map(response_dict)
            ScanMetrics.LAST_SUCCESSFUL_REQUEST = time.time()
            ScanMetrics.CONSECUTIVE_MAP_FAILS = 0
            log.debug("Parsed & saved.")
        except Exception as e:  # make sure we dont crash in the main loop
            log.error(e)
            log.error('Unexpected error while parsing response.')
            log.error('Response dict: {}'.format(response_dict))
            ScanMetrics.CONSECUTIVE_MAP_FAILS += 1
        else:
            ScanMetrics.STEPS_COMPLETED += 1
            if ScanMetrics.NUM_STEPS:
                ScanMetrics.CURRENT_SCAN_PERCENT = float(ScanMetrics.STEPS_COMPLETED) / ScanMetrics.NUM_STEPS * 100
            else:
                ScanMetrics.CURRENT_SCAN_PERCENT = 0
            log.info('Completed {:5.2f}% of scan.'.format(ScanMetrics.CURRENT_SCAN_PERCENT))

    def scan(self):
        ScanMetrics.NUM_STEPS = len(self.scan_config.COVER)
        log.info("Starting scan of {} locations in {} sec".format(ScanMetrics.NUM_STEPS, 30))
        # gSleep(30)

        for i, next_pos in enumerate(self.next_position()):
            log.debug('Scanning step {:d} of {:d}.'.format(i, ScanMetrics.NUM_STEPS))
            log.debug('Scan location is {} -> {:f}, {:f}'.format(next_pos[2], next_pos[0], next_pos[1]))

            # TODO: Add error throttle

            cell_ids = get_cell_ids(next_pos[0], next_pos[1], radius=70)
            timestamps = [0, ] * len(cell_ids)
            self.api.get_map_objects(
                latitude=f2i(next_pos[0]),
                longitude=f2i(next_pos[1]),
                cell_id=cell_ids,
                since_timestamp_ms=timestamps,
                position=next_pos,
                callback=Scanner.callback,
                username=next_pos[2])

        while not self.api.is_work_queue_empty():
            # Location change
            if self.scan_config.RESTART:
                log.info("Restarting scan")
                self.api.empty_work_queue()
            else:
                time.sleep(2)

        self.api.wait_until_done()  # Work queue empty != work done

    def run(self):
        while True:
            if self.scan_config.RESTART:
                self.scan_config.RESTART = False
                if self.scan_config.ACCOUNTS_CHANGED:
                    self.scan_config.ACCOUNTS_CHANGED = False
                    # count(accs) / 23 clamped in [3, 10]
                    num_workers = 1# min(max(int(math.ceil(len(config['ACCOUNTS']) / 23.0)), 3), 10)
                    self.api.resize_workers(num_workers)
                    self.api.add_accounts(config['ACCOUNTS'])

                    ScanMetrics.NUM_THREADS = num_workers
                    ScanMetrics.NUM_ACCOUNTS = len(config['ACCOUNTS'])

            if (not self.scan_config.SCAN_LOCATIONS or
                    not config.get('ACCOUNTS', None)):
                gSleep(5)
                continue
            ScanMetrics.STEPS_COMPLETED = 0
            scan_start_time = time.time()
            self.scan()
            ScanMetrics.COMPLETE_SCAN_TIME = time.time() - scan_start_time


class ScanConfig(object):
    SCAN_LOCATIONS = {}
    COVER = None

    RESTART = True  # Triggered when the setup is changed due to user input
    ACCOUNTS_CHANGED = True

    def update_scan_locations(self, scan_locations):
        location_names = set([])
        # Add new locations
        for scan_location in scan_locations:
            if scan_location['location'] not in self.SCAN_LOCATIONS:
                if ('latitude' not in scan_location or
                        'longitude' not in scan_location or
                        'altitude' not in scan_location):
                    lat, lng, alt = get_pos_by_name(scan_location['location'])
                    log.info('Parsed location is: {:.4f}/{:.4f}/{:.4f} '
                             '(lat/lng/alt)'.format(lat, lng, alt))
                    scan_location['latitude'] = lat
                    scan_location['longitude'] = lng
                    scan_location['altitude'] = alt
                self.SCAN_LOCATIONS[scan_location['location']] = scan_location
            location_names.add(scan_location['location'])

        # Remove old locations
        for location_name in self.SCAN_LOCATIONS:
            if location_name not in location_names:
                del self.SCAN_LOCATIONS[location_name]

        self._update_cover()

    def add_scan_location(self, lat, lng, radius):
        scan_location = {
            'location': '{},{}'.format(lat, lng),
            'latitude': lat,
            'longitude': lng,
            'altitude': 0,
            'radius': radius
        }

        self.SCAN_LOCATIONS[scan_location['location']] = scan_location
        self._update_cover()

    def delete_scan_location(self, lat, lng):
        for k, v in self.SCAN_LOCATIONS.iteritems():
            if v['latitude'] == lat and v['longitude'] == lng:
                del self.SCAN_LOCATIONS[k]
                self._update_cover()
                return

    # TODO: use global grid instead of per point - this can eliminate overlapping problems
    def _update_cover(self):
        cover = []
        cell_count = 0
        region_count = len(self.SCAN_LOCATIONS.values())

        # Go backwards through locations so that last location
        # will be scanned first
        for scan_location in reversed(self.SCAN_LOCATIONS.values()):
            lat = scan_location["latitude"]
            lng = scan_location["longitude"]
            radius = scan_location["radius"]

            d = math.sqrt(3) * 70
            points = [[{'lat2': lat, 'lon2': lng, 's': 0, 'verts': []}]]

            for c in range(0, 6):
                angle = 30 + 60 * c
                vertex = Geodesic.WGS84.Direct(points[0][0]['lat2'], points[0][0]['lon2'], angle, 70)
                points[0][0]['verts'].append((vertex['lat2'], vertex['lon2']))

            # The lines below are magic. Don't touch them.

            # i is the i'th layer of hexagons, we stop iterating when all of the generated hexagons's
            # centers are outside of the radius
            for i in xrange(1, maxint):
                oor_counter = 0

                layer = []
                # for each new cell in the layer get the one of the previous
                # and angle to it's center and calculate new cell center
                for j in range(0, 6 * i):
                    prev_idx = (j - j / i - 1 + (j % i == 0))
                    angle_to_prev = (j+i-1)/i * 60

                    p = points[i - 1][prev_idx]
                    p_new = Geodesic.WGS84.Direct(p['lat2'], p['lon2'], angle_to_prev, d)
                    p_new['s'] = Geodesic.WGS84.Inverse(p_new['lat2'], p_new['lon2'], lat, lng)['s12']
                    layer.append(p_new)

                    if p_new['s'] > radius:
                        oor_counter += 1

                points.append(layer)
                if oor_counter == 6 * i:
                    break

            points.pop()
            # get the last i elements and push them on the front, so every layer starts
            # one cell anti clockwise before the previous
            for c, layer in enumerate(points):
                for f in range(0, 0):
                    layer = [layer.pop()] + layer
                points[c] = layer

            cells = [{"lat": p['lat2'], "lng": p['lon2']} for sublist in points for p in sublist]
            cell_count += len(cells)
            cover.append(cells)

        log.info("we have {} cells in {} regions".format(cell_count, region_count))
        self._make_paths(cover, cell_count)
        self.COVER = self._get_cover_with_cells(cover)

    def _get_cover_with_cells(self, cover):
        cell_cover = []
        for patch in cover:
            for cell in patch:
                vertices = []
                for c in range(0, 6):
                    angle = 30 + 60 * c
                    vertex = Geodesic.WGS84.Direct(cell['lat'], cell['lng'], angle, 70)
                    vertices.append((vertex['lat2'], vertex['lon2']))
                cell['verts'] = vertices
                cell_cover.append(cell)
        return cell_cover

    def _make_paths(self, cover, cell_count):
        accs = config['ACCOUNTS']
        acc_count = len(accs)
        region_count = len(cover)

        if acc_count < region_count:
            log.error("You have less accounts than regions - reduce region count or add accounts")


        unames = [acc['username'] for acc in accs]

        # give every region atleast 1 acc
        region_accs = [1 for _ in cover]
        remaining = acc_count - len(region_accs)

        # calculate possible extra accs
        for c, region in enumerate(cover):
            region_cells = len(region)
            # region_weight * acc_count = accs for this region
            print("region_cells:{} cell_count:{} acc_count:{}".format(region_cells, cell_count, acc_count))
            extra_acc = (float(region_cells) / float(cell_count)) * float(acc_count)
            # find how much this region gets and remove the guaranteed 1
            extra_acc = int(max(extra_acc, 1)) - 1
            # clamp to remaining
            extra_acc = min(extra_acc, remaining)
            region_accs[c] += extra_acc
            # clamp at 0 min
            remaining -= extra_acc
            # no extras left, leave
            if remaining <= 0:
                break

        acc_counter = 0
        for c, region in enumerate(cover):
            region_workers = region_accs[c]
            cell_count = len(region)
            cells_per_acc = cell_count / region_workers

            idx = 0
            for acc_idx in range(acc_counter, acc_counter + region_workers):
                for cell_idx in range(idx, idx + cells_per_acc):
                    region[cell_idx]['acc'] = unames[acc_idx]

                # move offset
                idx += cells_per_acc

            acc_counter += region_workers
            # give the last worker the remaining
            for c in range(idx, cell_count):
                region[c]['acc'] = unames[acc_counter - 1]
