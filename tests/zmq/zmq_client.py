from time import sleep

import zmq

PORT = 9124


def main():
    """Main.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    print('Connect port %s' % PORT)
    socket.connect("tcp://localhost:%s" % PORT)

    while True:
        socket.send_json({'a': 1})
        sleep(5)
        socket.recv()


if __name__ == '__main__':
    main()
