"""
pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.

Author: tjado <https://github.com/tejado>
"""

# from __future__ import absolute_import

import re
import six
import logging
import requests
import time
import math
import sys
from threading import Thread, Lock as Mutex
from Queue import Queue, PriorityQueue, LifoQueue
from collections import deque

from . import __title__, __version__, __copyright__
from .rpc_api import RpcApi
from .auth_ptc import AuthPtc
from .auth_google import AuthGoogle
from .exceptions import AuthException, NotLoggedInException, ServerBusyOrOfflineException, NoPlayerPositionSetException, EmptySubrequestChainException, ServerApiEndpointRedirectException, AuthTokenExpiredException

from . import protos
from POGOProtos.Networking.Requests_pb2 import RequestType

logger = logging.getLogger(__name__)

class TaskQueue:
    def __init__(self):
        # will map username -> stack of tasks
        self.data = {}
        self.mutex = Mutex()

    def push(self, user, item, reAdd=False):
        self.mutex.acquire()
        if user not in self.data:
            self.data[user] = (Mutex(), LifoQueue())
        self.data[user][1].put(item)
        # returning task - unlock user and mark task done in stack
        if reAdd:
            self.data[user][1].task_done()
            self.data[user][0].release()
        self.mutex.release()

    def __len__(self):
        n = 0
        self.mutex.acquire()
        for item in self.data.values():
            n += item[1].qsize()
        self.mutex.release()
        return n

    def pop(self, user):
        item = None
        self.mutex.acquire()
        if not user in self.data:
            sys.stderr.write("Popping task for {} but it is not in task queue!".format(user))
            sys.stderr.flush()
            sys.exit(1)
        lock, que = self.data[user]
        lock.acquire()
        item = que.get()
        self.mutex.release()
        return item

    def clear(self):
        self.mutex.acquire()
        for _, item in self.data.iteritems():
            item[0].acquire()
            while not item[1].empty():
                try:
                    item[1].get(False)
                    item[1].task_done()
                except Queue.Empty:
                    break
            item[0].release()
        self.mutex.release()

    def popRandom(self):
        acc = None
        item = None
        with self.mutex:
            for name, data in self.data.iteritems():
                if not data[0].locked() and not data[1].empty():
                    data[0].acquire()
                    if data[1].empty():
                        data[0].release()
                        continue
                    else:
                        acc = name
                        item = data[1].get()
        return acc, item

    def done(self, user):
        self.mutex.acquire()
        if not user in self.data:
            sys.stderr.write("Marking task done for {} but it is not in task queue!".format(user))
            sys.stderr.flush()
            sys.exit(1)
        self.data[user][1].task_done()
        self.data[user][0].release()
        self.mutex.release()

    def join(self):
        self.mutex.acquire()
        for item in self.data.values():
            item[0].acquire()
            item[1].join()
            item[0].release()
        self.mutex.release()


class PGoApi:
    def __init__(self, signature_lib_path):
        self.set_logger()

        self._signature_lib_path = signature_lib_path

        self._tasks = TaskQueue()
        self._auths = {}

        self._workers = []
        self._api_endpoint = 'https://pgorelease.nianticlabs.com/plfe/rpc'

        self.log.info('%s v%s - %s', __title__, __version__, __copyright__)

    def create_workers(self, num_workers):
        for i in xrange(num_workers):
            worker = PGoApiWorker(self._signature_lib_path, self._tasks, self._auths)
            worker.daemon = True
            worker.start()
            self._workers.append(worker)

    def resize_workers(self, num_workers):
        workers_now = len(self._workers)
        if workers_now < num_workers:
            self.create_workers(num_workers - workers_now)
        elif workers_now > num_workers:
            for i in xrange(workers_now - num_workers):
                worker = self._workers.pop()
                worker.stop()

    def add_accounts(self, accounts):
        for account in accounts:
            username, password = account['username'], account['password']
            if not isinstance(username, six.string_types) or not isinstance(password, six.string_types):
                raise AuthException("Username/password not correctly specified")

            provider = account.get('provider', 'ptc')
            if provider == 'ptc':
                auth_provider = AuthPtc(username, password)
            elif provider == 'google':
                auth_provider = AuthGoogle(username, password)
            else:
                raise AuthException("Invalid authentication provider - only ptc/google available.")

            self._auths[username] = (time.time(), auth_provider)

    def set_logger(self, logger=None):
        self.log = logger or logging.getLogger(__name__)

    def get_api_endpoint(self):
        return self._api_endpoint

    def __getattr__(self, func):
        def function(**kwargs):
            name = func.upper()

            position = kwargs.pop('position')
            callback = kwargs.pop('callback')
            acc = kwargs.pop('username')

            if acc not in self._auths:
                self.log.error('Failed to add task for {}, no such auth'.format(acc))
                raise AttributeError

            if kwargs:
                method = {RequestType.Value(name): kwargs}
                self.log.debug(
                   "Adding '%s' to RPC request including arguments", name)
                self.log.debug("Arguments of '%s': \n\r%s", name, kwargs)
            else:
                method = RequestType.Value(name)
                self.log.debug("Adding '%s' to RPC request", name)

            self.call_method(method, position, callback, acc)

        if func.upper() in RequestType.keys():
            return function
        else:
            raise AttributeError

    def call_method(self, method, position, callback, acc):
        self._tasks.push(acc, (method, position, callback))

    def empty_work_queue(self):
        while len(self._tasks):
            self._tasks.popRandom()

    def is_work_queue_empty(self):
        return 0 == len(self._tasks)

    def wait_until_done(self):
        self._tasks.join()



class PGoApiWorker(Thread):
    THROTTLE_TIME = 10.0
    # In case the server returns a status code 3, this has to be requested
    SC_3_REQUESTS = [RequestType.Value("GET_PLAYER")]

    def __init__(self, signature_lib_path, task_queue, auths):
        Thread.__init__(self)
        self.log = logging.getLogger(__name__)
        self._running = True

        self.auths = auths
        self.tasks = task_queue
        self.rpc_api = RpcApi(None)
        self.rpc_api.activate_signature(signature_lib_path)

    def run(self):
        def abort(acc, item):
            self.tasks.push(acc, item, reAdd=True)

        didWork = True
        while self._running:
            if not didWork:
                time.sleep(0.1)
            didWork = False

            acc, item = self.tasks.popRandom()
            if not self._running:
                abort(acc, item)
                continue

            if not acc in self.auths:
                continue

            next_call, auth_provider = self.auths[acc]
            if time.time() < next_call:
                # need to wait with this 1
                abort(acc, item)
                continue


            self.log.info("Will work with [{}] -> [{}]".format(acc, item))
            method, position, callback = item

            # Let's do this.
            self.rpc_api._auth_provider = auth_provider
            try:
                response = self.call(auth_provider, [method], position)
                next_call = time.time() + self.THROTTLE_TIME
            except Exception as e:
                # Too many login retries lead to an AuthException
                # So let us sideline this auth provider for 5 minutes
                if isinstance(e, AuthException):
                    self.log.error("AuthException in worker thread. Username: {}".format(auth_provider.username))
                    next_call = time.time() + 5 * 60
                else:
                    self.log.error("Error in worker thread. Returning empty response. Error: {}".format(e))
                    next_call = time.time() + self.THROTTLE_TIME

                abort(acc, item)
                response = {}

            self.tasks.done(acc)
            self.rpc_api._auth_provider = None

            didWork = True
            callback(response)

    def stop(self):
        self._running = False

    def call(self, auth_provider, req_method_list, position):
        if not req_method_list:
            raise EmptySubrequestChainException()

        lat, lng, alt = position
        if (lat is None) or (lng is None) or (alt is None):
            raise NoPlayerPositionSetException()

        self.log.debug('Execution of RPC')
        response = None

        again = True  # Status code 53 or not logged in?
        retries = 5
        while again:
            self._login_if_necessary(auth_provider, position)

            try:
                response = self.rpc_api.request(auth_provider.get_api_endpoint(), req_method_list, position)
                if not response:
                    raise ValueError('Request returned problematic response: {}'.format(response))
            except NotLoggedInException:
                pass  # Trying again will call _login_if_necessary
            except AuthTokenExpiredException:
                auth_provider._ticket_expire = time.time()
            except ServerApiEndpointRedirectException as e:
                auth_provider.set_api_endpoint('https://{}/rpc'.format(e.get_redirected_endpoint()))
            except Exception as e:  # Never crash the worker
                if isinstance(e, ServerBusyOrOfflineException):
                    self.log.info('Server seems to be busy or offline!')
                else:
                    self.log.info('Unexpected error during request: {}'.format(e))
                if retries == 0:
                    return {}
                retries -= 1
            else:
                if 'api_url' in response:
                    auth_provider.set_api_endpoint('https://{}/rpc'.format(response['api_url']))

                if 'status_code' in response and response['status_code'] == 3:
                    self.log.info("Status code 3 returned. Performing get_player request.")
                    req_method_list = self.SC_3_REQUESTS + req_method_list
                elif 'responses' in response and not response['responses']:
                    self.log.info("Received empty map_object response. Logging out and retrying.")
                    auth_provider._ticket_expire = time.time() # this will trigger a login in _login_if_necessary
                else:
                    again = False

        return response

    def _login(self, auth_provider, position):
        self.log.info('Attempting login: {}'.format(auth_provider.username))
        consecutive_fails = 0

        while not auth_provider.user_login():
            sleep_t = min(math.exp(consecutive_fails / 1.7), 5 * 60)
            self.log.info('Login failed, retrying in {:.2f} seconds'.format(sleep_t))
            consecutive_fails += 1
            time.sleep(sleep_t)
            if consecutive_fails == 5:
                raise AuthException('Login failed five times.')

        self.log.info('Login successful: {}'.format(auth_provider.username))

    def _login_if_necessary(self, auth_provider, position):
        if auth_provider._ticket_expire:
            remaining_time = auth_provider._ticket_expire / 1000 - time.time()

            if remaining_time < 60:
                self.log.info("Login for {} has or is about to expire".format(auth_provider.username))
                self._login(auth_provider, position)
        else:
            self._login(auth_provider, position)
