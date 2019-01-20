from time import sleep

import zmq

PORT = 9123


def main():
    """Main.
    """
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    print('Binding port %s' % PORT)
    socket.bind("tcp://*:%s" % PORT)

    while True:
        socket.send(b"a abc")
        sleep(1)


if __name__ == '__main__':
    main()
