import zmq

PORT = 9124


def main():
    """Main.
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    print('Binding port %s' % PORT)
    socket.bind("tcp://*:%s" % PORT)

    while True:
        message = socket.recv()
        print(message)
        socket.send(message)


if __name__ == '__main__':
    main()
