from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Collection, List, Tuple


from pydantic import BaseModel
from tiingo import TiingoClient

tiingo_client = TiingoClient()


class Position(BaseModel):
    name: str
    size: int
    cost_basis: float
    time_opened: datetime

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Position):
            return self.name.lower() == other.name.lower()
        elif isinstance(other, str):
            return self.name.lower() == other.lower()


class ClosedPosition(Position):
    proceeds: float
    time_closed: datetime


class AccountAction(BaseModel):
    type: str
    amount: float
    date: datetime


class OrderStatus(Enum):
    OPEN = "open"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELED = "canceled"
    PENDING = "pending"
    PARTIALLY_FILLED = "partially_filled"
    CALCULATED = "calculated"
    ACCEPTED_FOR_BIDDING = "accepted_for_bidding"
    ACCEPTED = "accepted"
    ERROR = "error"
    HELD = "held"
    PENDING_NEW = "pending_new"


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"


class MarketDay(BaseModel):
    open: datetime
    close: datetime


class Order(BaseModel):
    id: str
    name: str
    side: str
    type: str
    status: OrderStatus
    executed_quantity: int
    avg_fill_price: float

    @property
    def cost(self):
        return self.executed_quantity * self.avg_fill_price


class Quote(BaseModel):
    name: str
    price: float


class AccountBalance:
    def __init__(
        self,
        total_cash: float,
        total_equity: float,
        open_pl: float,
        long_value: float,
        settled_cash: float,
    ):
        self._total_cash = total_cash
        self._total_equity = total_equity
        self._open_pl = open_pl
        self._long_value = long_value
        self._settled_cash = settled_cash

    @property
    def total_cash(self):
        return self._total_cash

    @property
    def total_equity(self):
        return self._total_equity

    @property
    def open_pl(self):
        return self._open_pl

    @property
    def long_value(self):
        return self._long_value

    @property
    def settled_cash(self):
        return self._settled_cash


class ReturnStream:
    def __init__(
        self,
        closed_positions: List[ClosedPosition],
        admin_adjustments: List[Tuple[datetime, float]],
    ):
        self._initial = closed_positions[0]

        position_gains = [(x.time_closed, x.proceeds - x.cost_basis) for x in closed_positions]

        grouped_dollar_gains = defaultdict(float)
        for dt, gl in position_gains + admin_adjustments:
            grouped_dollar_gains[dt.date()] += gl

        self._gains = sorted(grouped_dollar_gains.items(), key=lambda x: x[0])

    @staticmethod
    def __percent_change(start, end):
        return ((end - start) / start) * 100

    @property
    def total_return(self) -> float:
        return ReturnStream.__percent_change(self._initial, sum(x[1] for x in self._gains))

    @property
    def ytd_return(self) -> float:
        current_year = datetime.utcnow().year
        starting_amount = sum(x[1] for x in self._gains if x[0].year < current_year)
        current_amount = sum(x[1] for x in self._gains)
        return ReturnStream.__percent_change(starting_amount, current_amount)

    @property
    def returns(self) -> Collection[Tuple[datetime, float]]:
        last_value = self._initial
        percentage_returns = []
        for dt, gl in self._gains:
            rtn = ReturnStream.__percent_change(self._initial, last_value + gl)
            percentage_returns.append((dt, rtn))
            last_value += gl
        return percentage_returns


class Broker(ABC):
    def __init__(self, account_number: str):
        self._account_number = account_number

    def __str__(self):
        return f"{self.__class__.__name__}({self._account_number})"

    @abstractmethod
    async def place_market_sell(self, name: str, quantity: int):
        pass

    @abstractmethod
    async def place_market_buy(self, name: str, quantity: int):
        pass

    @property
    @abstractmethod
    async def positions(self) -> List[Position]:
        pass

    @abstractmethod
    async def get_quote(self, name: str) -> Quote:
        pass

    @abstractmethod
    async def get_quotes(self, names: Collection[str]) -> List[Quote]:
        pass

    @property
    @abstractmethod
    async def account_balance(self) -> AccountBalance:
        pass

    @property
    @abstractmethod
    async def orders(self) -> Collection[Order]:
        pass

    @abstractmethod
    async def cancel_order(self, order_id):
        pass

    @property
    @abstractmethod
    async def account_pnl(self) -> ReturnStream:
        pass

    @abstractmethod
    async def account_history(self):
        pass

    @abstractmethod
    async def calendar(self) -> List[MarketDay]:
        pass
