try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import json
from api_socket import ApiSocket

class RESTfulApiSocket(ApiSocket):
    """
    Generic REST API call
    """
    def __init__(self):
        """
        Constructor
        """
        ApiSocket.__init__(self)

    def request(self, url):
        """
        Web request
        :param: url: The url link
        :return JSON object
        """
        res = urlrequest.urlopen(url)
        res = json.loads(res.read().decode('utf8'))
        return res
        
    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = L2Depth(exch=instmt.get_exchange_name(), instmt=instmt.get_instmt_code())
        field_map = instmt.get_restful_order_book_fields_mapping()
        for key, value in raw.items():
            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from restful_order_book_fields_mapping on key %s" % key)
                    raise
                
                if field == 'TIMESTAMP':
                    offset = 1 if 'TIMESTAMP_OFFSET' not in field_map else field_map['TIMESTAMP_OFFSET']
                    if offset == 1:
                        date_time = float(value)
                    else:
                        date_time = float(value)/offset
                    l2_depth.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
                elif field == 'BIDS':
                    bids = value
                    sorted(bids, key=lambda x: x[0])
                    l2_depth.bid = [float(e[0]) if type(e[0]) != float else e[0] for e in bids]
                    l2_depth.bid_volume = [float(e[1]) if type(e[1]) != float else e[1] for e in bids]
                elif field == 'ASKS':
                    asks = value
                    sorted(asks, key=lambda x: x[0], reverse=True)
                    l2_depth.ask = [float(e[0]) if type(e[0]) != float else e[0] for e in asks]
                    l2_depth.ask_volume = [float(e[1]) if type(e[1]) != float else e[1] for e in asks]
                else:
                    raise Exception('The field <%s> is not found' % field)

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade(exch=instmt.get_exchange_name(), instmt=instmt.get_instmt_code())
        field_map = instmt.get_restful_trades_fields_mapping()
        for key, value in raw.items():
            if key in field_map.keys():
                try:
                    field = field_map[key]
                except:
                    print("Error from get_restful_trades_fields_mapping on key %s" % key)
                    raise
                
                if field == 'TIMESTAMP':
                    offset = 1 if 'TIMESTAMP_OFFSET' not in field_map else field_map['TIMESTAMP_OFFSET']
                    if offset == 1:
                        date_time = float(value)
                    else:
                        date_time = float(value)/offset
                    trade.date_time = datetime.utcfromtimestamp(date_time).strftime("%Y%m%d %H:%M:%S.%f")
                elif field == 'TRADE_SIDE':
                    side = value
                    if type(side) != int:
                        side = side.lower()
                        if side == 'buy':
                            side = 1
                        elif side == 'sell':
                            side = 2
                        else:
                            raise Exception('Unrecognized trade side %s' % side)
                    
                    if side == 1:
                        trade.trade_side = trade.Side.BUY
                    elif side == 2:
                        trade.trade_side = trade.Side.SELL
                    else:
                        print(side)
                        raise Exception('Unexpected trade side value %d' % side)
                        
                elif field == 'TRADE_ID':
                    trade.trade_id = value
                elif field == 'TRADE_PRICE':
                    trade.trade_price = value
                elif field == 'TRADE_VOLUME':
                    trade.trade_volume = value
                else:
                    raise Exception('The field <%s> is not found' % field)        

        return trade

    def get_order_book(self, instmt):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        res = self.request(instmt.get_restful_order_book_link())
        return self.parse_l2_depth(instmt=instmt,
                                   raw=res)

    def get_trades(self, instmt, trade_id):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        res = self.request(instmt.get_restful_trades_link().replace('<id>', str(trade_id)))
        trades = []
        if len(res) > 0:
            for t in res:
                trade = self.parse_trade(instmt=instmt,
                                         raw=t)
                trades.append(trade)

        return trades    

