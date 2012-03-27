import argparse

from gevent_zeromq import zmq

context = zmq.Context()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Actor broker.')
    parser.add_argument("--request-address", default="localhost",
            dest="request_address", help="The address the publisher listens on")
    parser.add_argument("--request-port", default=5558, dest="request_port",
            type=int, help="The port the publisher listens on")
    parser.add_argument("--response-address", default="*",
            dest="response_address", help="The address the pusher should listen on")
    parser.add_argument("--response-port", default=5559, dest="response_port",
            type=int, help="The port the pusher should listen on")
    parser.add_argument("name", help="The actor name to broke")
    args = parser.parse_args()
    requestQ = context.socket(zmq.SUB)
    requestQ.connect("tcp://%s:%s" % (args.request_address, args.request_port))
    requestQ.setsockopt(zmq.SUBSCRIBE, args.name)
    actorSocket = context.socket(zmq.REQ)
    actorSocket.bind("tcp://%s:%s" % (args.response_address, args.response_port))

    while True:
        print "Waiting for work..."
        parts = requestQ.recv_multipart()
        print "Pushing %s" % parts
        actorSocket.send_multipart(parts)
        reply = actorSocket.recv()
        print "Got reply", reply
