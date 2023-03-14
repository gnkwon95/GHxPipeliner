import json

# color
WHITE = 1
BLACK = 0

# shape
DOJI = 0
SPINNING_TOP = 1
NORMAL = 2
MARUBOZU = 3

# buy or sell
BUY = 1
STAY = 0
SELL = -1

# chart history
RED = 1
NEUTRAL = 0
BLUE = -1

class Candle:
    def __init__(self, o: float, h: float, l: float, c: float):
        self.open_price = o
        self.high_price = h
        self.low_price = l
        self.close_price = c
        self.candle_size= h - o
        self.candle_color = self._get_candle_color()
        self.candle_shape = self._get_candle_shape()

    def _get_candle_color(self):
        if self.open_price < self.close_price:
            return WHITE
        else:
            return BLACK

    def _get_candle_shape(self):
        if self.candle_color == WHITE:
            self.top_shadow = self.high_price - self.close_price
            self.bot_shadow = self.open_price - self.low_price
            self.body_size = self.close_price = self.open_price
        elif self.candle_color == BLACK:
            self.top_shadow = self.high_price - self.open_price
            self.bot_shadow = self.close_price - self.low_price
            self.body_size = self.open_price = self.close_price
        else:
            raise Exception("no color")

        shadow_size = self.top_shadow + self.bot_shadow

        if shadow_size < self.candle_size * 0.1:
            return MARUBOZU
        
        elif shadow_size >= self.candle_size * 0.9:
            return DOJI
        
        elif shadow_size >= self.candle_size * 0.5:
            return SPINNING_TOP
        
        else:
            return NORMAL

    def determine_history(self, first_open_price, first_close_price, latest_open_price, latest_close_price):
        if first_close_price < latest_close_price and first_open_price < first_close_price and latest_open_price < latest_close_price:
            self.history = RED
        elif first_close_price > latest_close_price and first_open_price > first_close_price and latest_open_price > latest_close_price:
            self.history = BLUE
        else: 
            self.history = NEUTRAL

    def get_candle_info(self):
        return (self.candle_color, self.candle_shape)
    
    def get_buy_or_sell(self):
        if self.candle_color == WHITE and self.candle_size == MARUBOZU: # candle_type == '클로징 화이트 마루보주': 
            return BUY
        elif self.candle_color == WHITE and self.candle_size == NORMAL: # candle_type == '숏 바디 화이트':
            return STAY
        elif self.candle_size == DOJI and self.candle_size <= 1.1 * self.body_size: # candle_type == '포프라이스 도지':
            return STAY
        elif self.candle_size == DOJI and self.top_shadow > self.bot_shadow: # candle_type == '그레이브 스톤도지':
            if self.history == RED: # precandles is '상승세':
                return SELL
            else:
                return BUY
        elif self.candle_color == BLACK and self.candle_shape == MARUBOZU: # candle_type == '오프닝 블랙 마루보주':
            if self.history == RED: # precandles is '상승세':
                return BUY
            elif self.history == BLUE: 
                return STAY
            else: # candle_type == '블랙 마루보주':
                return SELL
        elif self.candle_color == BLACK and self.candle_size == NORMAL:# candle_type == '숏 바디 블랙':
            return STAY
        elif self.candle_shape == DOJI: # candle_type == '도지':
            return STAY # but unstable
        elif self.candle_size == DOJI and self.top_shadow < self.bot_shadow:  # candle_type == '드래곤 플라이 도지':
            if self.history == RED: # precandles is '상승세':
                return SELL
            else:
                return BUY
        else:
            return STAY