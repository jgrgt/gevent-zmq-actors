import argparse
import hashlib
import json

from gevent_zeromq import zmq

context = zmq.Context()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Act.')
    parser.add_argument("--request-address", default="localhost",
            dest="request_address", help="The address this actor will pull from")
    parser.add_argument("--request-port", default=5559, dest="request_port",
            type=int, help="The port this actor will pull from")
    parser.add_argument("--response-address", default="localhost", dest="response_address", help="The address the puller listens on")
    parser.add_argument("--response-port", default=5560, dest="response_port", type=int, help="The port the puller listens on")
    args = parser.parse_args()
    actorSocket = context.socket(zmq.REP)
    actorSocket.connect("tcp://%s:%s" % (args.request_address, args.request_port))
    responseQ = context.socket(zmq.PUSH)
    responseQ.connect("tcp://%s:%s" % (args.response_address, args.response_port))

    while True:
        print "Waiting for work..."
        name, requestId, body, rawParams = actorSocket.recv_multipart()
        actorSocket.send("OK")
        params = json.loads(rawParams)
        print "Got request to process %s" % params
        response = [hashlib.md5(r).hexdigest() for r in params.get('data', [])]
        responseQ.send_multipart((requestId, json.dumps(response)))
