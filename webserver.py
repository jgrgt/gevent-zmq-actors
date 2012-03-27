import uuid
import sys

import gevent
from gevent.pywsgi import WSGIServer
from gevent.event import AsyncResult
from gevent_zeromq import zmq

context = zmq.Context()

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
        body = getBody(env)
        requestId = generateRequestId()
        message = [path, requestId, body]
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
            (address, requestId, response) = self.responseQ.recv_multipart()
            log("Got response to %s" % requestId)
            ar = self.results.pop(requestId)
            ar.set(response)

if __name__ == "__main__":
    address = sys.argv[1]
    port = int(sys.argv[2])
    requestQ = context.socket(zmq.PUB)
    requestQ.bind("tcp://*:5558")
    responseQ = context.socket(zmq.PULL)
    responseQ.bind("tcp://*:5559")
    app = Application(requestQ, responseQ)
    server = WSGIServer((address, port), app.handler)
    print "Listening on %s:%s" % (address, port)
    server.serve_forever()
