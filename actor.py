import sys

from gevent_zeromq import zmq

context = zmq.Context()

if __name__ == "__main__":
    requestQ = context.socket(zmq.SUB)
    requestQ.connect("tcp://localhost:5558")
    name = sys.argv[1]
    requestQ.setsockopt(zmq.SUBSCRIBE, name)
    responseQ = context.socket(zmq.PUSH)
    responseQ.connect("tcp://localhost:5559")

    while True:
        print "Waiting for work..."
        path, requestId, body = requestQ.recv_multipart()
        print "Got request to %s" % path
        responseQ.send_multipart((path, requestId, "Actor %s got %s" % (name,
            body)))
