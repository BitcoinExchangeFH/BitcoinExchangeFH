from befh.api_socket import ApiSocket
from befh.util import Logger
import threading
from functools import partial
from socketIO_client import SocketIO

class SocketIOApiClient(ApiSocket):
    """
    Generic REST API call
    """
    def __init__(self, id):
        """
        Constructor
        :param id: Socket id
        """
        ApiSocket.__init__(self)
        self.ws = None              # Web socket
        self.id = id
        self.wst = None             # Web socket thread
        self._connected = False
        self.on_message_handlers = []
        self.on_open_handlers = []
        self.on_close_handlers = []
        self.on_error_handlers = []

    def connect(self, url,
                on_message_handler=None,
                on_open_handler=None,
                on_close_handler=None,
                on_error_handler=None):
        """
        :param url: Url link
        :param on_message_handler: Message handler which take the message as
                           the first argument
        :param on_open_handler: Socket open handler which take the socket as
                           the first argument
        :param on_close_handler: Socket close handler which take the socket as
                           the first argument
        :param on_error_handler: Socket error handler which take the socket as
                           the first argument and the error as the second
                           argument
        """
        Logger.info(self.__class__.__name__, "Connecting to socket <%s>..." % self.id)
        if on_message_handler is not None:
            self.on_message_handlers.append(on_message_handler)
        if on_open_handler is not None:
            self.on_open_handlers.append(on_open_handler)
        if on_close_handler is not None:
            self.on_close_handlers.append(on_close_handler)
        if on_error_handler is not None:
            self.on_error_handlers.append(on_error_handler)

        if not self._connected:
            self.wst = threading.Thread(target=lambda: self.run(url, 80))
            self.wst.start()

        return self.wst

    def run(self, url, port):
        while True:
            self.ws = SocketIO(url, port)
            self.ws.on('connect', self.__on_open)
            self.ws.on('request', self.__on_message)
            self.ws.on('message', self.__on_message)
            self.ws.on('disconnect', self.__on_close)
            try:
                while True:
                    self.ws.wait(1)
            except Exception as e:
                self._connected = False

    def send(self, msg):
        """
        Send message
        :param msg: Message
        :return:
        """
        self.ws.emit('request', msg)

    def __on_message(self, m):
        for handler in self.on_message_handlers:
            handler(m)

    def __on_open(self):
        Logger.info(self.__class__.__name__, "Socket <%s> is opened." % self.id)
        self._connected = True
        for handler in self.on_open_handlers:
            handler(self)
        
    def __on_close(self):
        Logger.info(self.__class__.__name__, "Socket <%s> is closed." % self.id)
        self._connected = False
        for handler in self.on_close_handlers:
            handler(self)
        
    def __on_error(self, error):
        Logger.info(self.__class__.__name__, "Socket <%s> error:\n %s" % (self.id, error))
        for handler in self.on_error_handlers:
            handler(self, error)
