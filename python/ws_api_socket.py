import websocket
import threading
from time import sleep
from api_socket import ApiSocket
from util import print_log

class WebSocketApiClient(ApiSocket):
    """
    Generic REST API call
    """
    def __init__(self, id):
        """
        Constructor
        :param id: Socket id
        """
        ApiSocket.__init__(self)
        self.ws = None
        self.id = id
        self.on_message_handler = None

    def connect(self, url, on_message_handler):
        """
        :param url: Url link
        :param on_message_handler: Message handler which take the message as
                           the first argument
        """
        print_log(self.__class__.__name__, "Connecting to socket <%s>..." % self.id)
        self.on_message_handler = on_message_handler
        self.ws = websocket.WebSocketApp(url,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error)
        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.start()

    def __on_message(self, ws, m):
        self.on_message_handler(m)

    def __on_open(self, ws):
        print_log(self.__class__.__name__, "Socket <%s> is opened." % self.id)
        
    def __on_close(self, ws):
        print_log(self.__class__.__name__, "Socket <%s> is closed." % self.id)
        
    def __on_error(self, ws, error):
        print_log(self.__class__.__name__, "Socket <%s> error:\n %s" % (self.id, error))

