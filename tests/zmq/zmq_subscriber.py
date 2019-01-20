import zmq

PORT = 9123


def main():
    """Main.
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    print('Connecting port %s' % PORT)
    socket.setsockopt(zmq.SUBSCRIBE, b'')
    socket.connect("tcp://localhost:%s" % PORT)
    print('Connected port %s' % PORT)

    while True:
        message = socket.recv()
        print("Message received: %s" % message)


if __name__ == '__main__':
    main()
