import argparse
import json
import urlparse
import uuid

import gevent
from gevent.pywsgi import WSGIServer
from gevent.event import AsyncResult
from gevent_zeromq import zmq

context = zmq.Context()

PATHS = {
            "/actors/rot13": "rot13",
            "/actors/md5sum": "md5"
        }


def getBody(env):
    body = ""
    try:
        length = int(env.get('CONTENT_LENGTH', '0'))
    except ValueError:
        length = 0
    if length:
        body = env['wsgi.input'].read(length)
    return body

def generateRequestId():
    return str(uuid.uuid4())

def log(msg):
    print msg

class Application(object):
    def __init__(self, requestQ, responseQ):
        self.requestQ = requestQ
        self.responseQ = responseQ
        self.results = {}
        gevent.spawn(self.waitForResponses)

    def handler(self, env, start_response):
        path = env['PATH_INFO']#, env['QUERY_STRING'], accept, start_response, errHandle)
        if path not in PATHS:
            start_response("404 Not Found", [("Content-Type", "text/plain")])
            return ["No actor handles path %s" % path]

        actorName = PATHS[path]
        body = getBody(env)
        qs = env.get('QUERY_STRING')
        if qs:
            params = urlparse.parse_qs(qs)
        else:
            params = {}

        requestId = generateRequestId()
        message = [actorName, requestId, body, json.dumps(params)]
        log("Sending %s" % message)
        self.requestQ.send_multipart(message)
        ar = AsyncResult()
        self.results[requestId] = ar
        result = ar.get()
        log("Got result %s" % result)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [result]

    def waitForResponses(self):
        while True:
            log("Waiting for a response")
            (requestId, response) = self.responseQ.recv_multipart()
            log("Got response to %s" % requestId)
            ar = self.results.pop(requestId)
            ar.set(response)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Serve some resources.')
    parser.add_argument("--address", "-a", default="0.0.0.0", help="The address the web server should listen on")
    parser.add_argument("--port", "-p", default=8080, type=int, help="The port the web server should listen on")
    parser.add_argument("--request-port", default=5558, dest="request_port", type=int, help="The port the publisher should listen on")
    parser.add_argument("--response-port", default=5560, dest="response_port", type=int, help="The port the puller should listen on")
    args = parser.parse_args()
    requestQ = context.socket(zmq.PUB)
    requestQ.bind("tcp://*:%s" % args.request_port)
    responseQ = context.socket(zmq.PULL)
    responseQ.bind("tcp://*:%s" % args.response_port)
    app = Application(requestQ, responseQ)
    server = WSGIServer((args.address, args.port), app.handler)
    print "Listening on %s:%s" % (args.address, args.port)
    server.serve_forever()
