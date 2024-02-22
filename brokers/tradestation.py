from typing import Collection, List
from datetime import datetime, timedelta

import httpx
from .broker import (
    MarketDay,
    AccountBalance,
    Position,
    Broker,
    Order,
    OrderStatus,
    OrderType,
    Quote,
    ReturnStream,
)
from tenacity import AsyncRetrying, stop_after_attempt

_order_type_map = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
}

_order_status_map = {"OPN": "open"}


class TradeStation(Broker):
    def __init__(self, account_number: str, **kwargs):
        super().__init__(account_number)

        self.refresh_token = kwargs.get("refresh_token")
        self.client_id = kwargs.get("client_id")
        self.client_secret = kwargs.get("client_secret")

        if self.refresh_token is None:
            raise ValueError("must have a refresh token to instantiate Tradestation broker")
        if self.client_id is None:
            raise ValueError("must have a client_id to instantiate Tradestation broker")
        if self.client_secret is None:
            raise ValueError("must have a client secret to instantiate Tradestation broker")

        self._api_url = kwargs.get("url", "api.tradestation.com").strip("/")
        self._signin_url = "signin.tradestation.com"
        self._api_version = kwargs.get("version", "v3").strip("/")

        self._access_token = None
        self._expiration_time = datetime.now()

    def _expired_token(self):
        return datetime.now() >= self._expiration_time

    async def _headers(self):
        if not self._access_token or self._expired_token():
            self._access_token, self._expiration_time = await self._refresh_token()

        return dict(Authorization=f"Bearer {self._access_token}")

    async def _refresh_token(self):
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        print(payload)
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(3)):
                with attempt:
                    resp = await client.post(
                        url=self._build_signin_url("oauth/token"),
                        data=payload,
                    )

        if not httpx.codes.is_success(resp.status_code):
            raise IOError(
                f"failed to refresh access_token Tradestation "
                f"with status code of {resp.status_code}: {resp.text}"
            )

        expires_in = timedelta(seconds=resp.json()["expires_in"])
        return resp.json()["access_token"], datetime.now() + expires_in

    def _build_url(self, path):
        return f"https://{self._api_url}/{self._api_version}/{path.strip('/')}"

    def _build_signin_url(self, path):
        return f"https://{self._signin_url}/{path.strip('/')}"

    async def _place_order(
        self,
        name: str,
        quantity: int,
        side: str,
        order_type: OrderType = OrderType.MARKET,
        stop_price: float = None,
    ) -> str:
        payload = {
            "Account": self._account_number,
            "Symbol": name,
            "TradeAction": side.upper(),
            "Quantity": quantity,
            "OrderType": _order_type_map.get(order_type),
            "TimeInForce": {"Duration": "GTC"},
        }

        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.post(
                        url=self._build_url("orderexecution/orders"),
                        json=payload,
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to place market {side} order from Tradestation "
                f"for {name} with a status code of"
                f" {response.status_code}: {response.text}"
            )
        if not response.json()["Orders"]:
            raise IOError(
                f"failed to place market {side} order from Tradestation "
                f"for {name} with a status code of"
                f" {response.status_code}: {response.json()}"
            )
        return response.json()["Orders"][0]["OrderID"]

    async def place_market_sell(self, name: str, quantity: int) -> str:
        return await self._place_order(name, quantity, "sell")

    async def place_market_buy(self, name: str, quantity: int) -> str:
        return await self._place_order(name, quantity, "buy")

    async def place_stop_loss(self, name: str, quantity: int, price: float):
        raise NotImplementedError("Still need to implement this")

    async def get_quote(self, name: str) -> Quote:
        quotes = await self.get_quotes([name])
        return quotes[0]

    async def get_quotes(self, names: Collection[str]) -> List[Quote]:
        if not names:
            return []

        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(f"/marketdata/quotes/{','.join(names)}"),
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get quotes from Tradestation for symbol(s) {names} "
                f"with a status code of {response.status_code}: {response.text}"
            )

        quotes = response.json()["Quotes"]

        if isinstance(quotes, list):
            return [Quote(name=quote["Symbol"], price=float(quote["Last"])) for quote in quotes]
        else:
            return [Quote(name=quotes["Symbol"], price=float(quotes["Last"]))]

    @property
    def account_number(self) -> str:
        return self._account_number

    @property
    async def account_balance(self) -> AccountBalance:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(f"brokerage/accounts/{self._account_number}/balances"),
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get account balance for account "
                f"{self._account_number} with a status code of "
                f"{response.status_code}: {response.text}"
            )

        balances = response.json()["Balances"][0]

        return AccountBalance(
            total_cash=float(balances["CashBalance"]),
            total_equity=float(balances["Equity"]),
            open_pl=float(balances["BalanceDetails"]["UnrealizedProfitloss"]),
            long_value=float(balances["MarketValue"]),
            settled_cash=None,
        )

    async def order_status(self, order_id: str) -> Order:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(
                            f"/brokerage/accounts/{self._account_number}/orders/{order_id}"
                        ),
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get order status for order {order_id} "
                f"{self._account_number} with a status code of "
                f"{response.status_code}: {response.text}"
            )

        order = response.json()["Orders"][0]

        return Order(
            id=order_id,
            name=order["Legs"][0]["Symbol"],
            side=order["Legs"][0]["BuyOrSell"],
            type=order["Legs"][0]["OrderType"],
            status=OrderStatus(order["Status"]),
            executed_quantity=int(float(order["Legs"][0]["ExecQuantity"])),
            avg_fill_price=None,
        )

    @property
    async def orders(self) -> Collection[Order]:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(f"brokerage/accounts/{self._account_number}/orders"),
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get orders with a status code of "
                f"{response.status_code}: {response.text}"
            )

        orders = response.json()["Orders"]
        orders = (
            []
            if orders == "null"
            else [orders["order"]]
            if type(orders["order"]) is dict
            else orders["order"]
        )

        return [
            Order(
                id=order["OrderID"],
                name=order["Legs"][0]["Symbol"],
                side=order["Legs"][0]["BuyOrSell"],
                type=order["OrderType"],
                status=OrderStatus(order["Status"]),
                executed_quantity=int(float(order["Legs"][0]["ExecQuantity"])),
                avg_fill_price=None,
            )
            for order in orders
        ]

    async def cancel_order(self, order_id):
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.delete(
                        url=self._build_url(f"/orderexecution/orders/{order_id}"),
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to delete order with a status code of "
                f"{response.status_code}: {response.text}"
            )

    @property
    async def account_pnl(self) -> ReturnStream:
        raise NotImplementedError()

    async def account_history(self):
        raise NotImplementedError()

    @property
    async def positions(self) -> List[Position]:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url("/brokerage/accounts/[[account]]/positions/"),
                        headers=await self._headers(),
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get account positions for account "
                f"{self._account_number} with a status code of "
                f"{response.status_code}: {response.text}"
            )

        positions = response.json()["Positions"]

        return [
            Position(
                name=pos["Symbol"],
                size=pos["Quantity"],
                cost_basis=pos["TotalCost"],
                time_opened=datetime.strptime(pos["Timestamp"], "%Y-%m-%dT%H:%M:%SZ"),
            )
            for pos in positions
        ]

    async def calendar(self) -> List[MarketDay]:
        raise NotImplementedError("calendar is not implemented")
