from .broker import Broker, AccountBalance, MarketDay, Position, Quote, Order, OrderStatus, ClosedPosition

from datetime import date, datetime
from typing import Collection, List

import httpx
from httpx import codes
from tenacity import AsyncRetrying, stop_after_attempt


class Tradier(Broker):
    def __init__(self, account_number: str, **kwargs):
        super().__init__(account_number)

        access_token = kwargs.get("access_token")
        if access_token is None:
            raise ValueError("must have an access token to instantiate Tradier broker")

        self._api_url = kwargs.get("url", "api.tradier.com").strip("/")
        self._api_version = kwargs.get("version", "v1").strip("/")

        self._headers = dict(Accept="application/json", Authorization=f"Bearer {access_token}")

    def _build_url(self, path):
        hydrated_path = path.replace("[[account]]", self._account_number).strip("/")
        return f"https://{self._api_url}/{self._api_version}/{hydrated_path}"

    async def _place_order(
        self,
        name: str,
        quantity: int,
        side: str,
        order_type: str = "market",
        stop_price: float = None,
    ) -> str:
        payload = {
            "class": "equity",
            "symbol": name,
            "side": side,
            "quantity": quantity,
            "type": order_type,
            "duration": "gtc",
        }

        if order_type in ("stop", "stop_limit"):
            payload["stop"] = stop_price

        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.post(
                        url=self._build_url("/accounts/[[account]]/orders"),
                        data=payload,
                        headers=self._headers,
                    )
                    x = response

        if response.status_code != codes.OK:
            raise IOError(
                f"failed to place market {side} order from Tradier "
                f"for {name} with a status code of"
                f" {response.status_code}: {response.text}"
            )
        if "order" not in response.json():
            raise IOError(
                f"failed to place market {side} order from Tradier "
                f"for {name} with a status code of"
                f" {response.status_code}: {response.text}"
            )
        return response.json()["order"]["id"]

    async def place_market_sell(self, name: str, quantity: int) -> str:
        return await self._place_order(name, quantity, "sell")

    async def place_market_buy(self, name: str, quantity: int) -> str:
        return await self._place_order(name, quantity, "buy")

    async def place_stop_loss(self, name: str, quantity: int, price: float):
        return await self._place_order(name, quantity, "sell", "stop", price)

    @property
    async def account_balance(self) -> AccountBalance:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url("/accounts/[[account]]/balances/"),
                        headers=self._headers,
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get account balance for account "
                f"{self._account_number} with a status code of "
                f"{response.status_code}: {response.text}"
            )

        balances = response.json()["balances"]

        return AccountBalance(
            total_cash=balances["total_cash"],
            total_equity=balances["total_equity"],
            open_pl=balances["open_pl"],
            long_value=balances["long_market_value"],
            settled_cash=balances["total_cash"] - balances["cash"]["unsettled_funds"]
            if balances["account_type"] == "cash"
            else None,
        )

    @property
    async def positions(self) -> List[Position]:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url("/accounts/[[account]]/positions/"),
                        headers=self._headers,
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get account positions for account "
                f"{self._account_number} with a status code of "
                f"{response.status_code}: {response.text}"
            )

        if response.json()["positions"] in (None, "null"):
            return list()

        positions = response.json()["positions"]["position"]

        if isinstance(positions, list):
            return [
                Position(
                    name=pos["symbol"],
                    size=pos["quantity"],
                    cost_basis=pos["cost_basis"],
                    time_opened=datetime.strptime(pos["date_acquired"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                )
                for pos in positions
            ]

        else:
            return [
                Position(
                    name=positions["symbol"],
                    size=positions["quantity"],
                    cost_basis=positions["cost_basis"],
                    time_opened=datetime.strptime(
                        positions["date_acquired"], "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                )
            ]

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
                        url=self._build_url("/markets/quotes/"),
                        params=dict(symbols=",".join(names), greeks=False),
                        headers=self._headers,
                    )

        if response.status_code != codes.OK:
            raise IOError(
                f"failed to get quotes from Tradier for symbol(s) {names} "
                f"with a status code of {response.status_code}: {response.text}"
            )

        quotes = response.json()["quotes"]["quote"]

        if isinstance(quotes, list):
            return [Quote(name=quote["symbol"], price=float(quote["last"])) for quote in quotes]
        else:
            return [Quote(name=quotes["symbol"], price=float(quotes["last"]))]

    async def order_status(self, order_id: str) -> Order:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(f"/accounts/[[account]]/orders/{order_id}"),
                        headers=self._headers,
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get order status for order {order_id} "
                f"{self._account_number} with a status code of "
                f"{response.status_code}: {response.text}"
            )

        order = response.json()["order"]

        return Order(
            id=order_id,
            name=order["symbol"],
            side=order["side"],
            type=order["type"],
            status=OrderStatus(order["status"]),
            executed_quantity=int(float(order["exec_quantity"])),
            avg_fill_price=float(order["avg_fill_price"]),
        )

    @property
    async def orders(self) -> Collection[Order]:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(f"/accounts/[[account]]/orders"), headers=self._headers
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to get orders with a status code of "
                f"{response.status_code}: {response.text}"
            )

        orders = response.json()["orders"]
        orders = (
            []
            if orders == "null"
            else [orders["order"]]
            if type(orders["order"]) is dict
            else orders["order"]
        )

        return [
            Order(
                id=order["id"],
                name=order["symbol"],
                side=order["side"],
                type=order["type"],
                status=OrderStatus(order["status"]),
                executed_quantity=int(float(order["exec_quantity"])),
                avg_fill_price=float(order["avg_fill_price"]),
            )
            for order in orders
        ]

    async def cancel_order(self, order_id):
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.delete(
                        url=self._build_url(f"/accounts/[[account]]/orders/{order_id}"),
                        headers=self._headers,
                    )

        if response.status_code != httpx.codes.OK:
            raise IOError(
                f"failed to delete order with a status code of "
                f"{response.status_code}: {response.text}"
            )

    async def account_pnl(self, since_date: date = None) -> List[ClosedPosition]:
        if since_date is None:
            params_ = None
        else:
            params_ = {"start": since_date.strftime("%Y-%m-%d")}

        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url("/accounts/[[account]]/gainloss"),
                        params=params_,
                        headers=self._headers,
                    )

        if response.status_code != codes.OK:
            raise IOError(
                f"failed to retrieve gainloss from Tradier account "
                f"with a status code of {response.status_code}: "
                f"{response.text}"
            )

        gainloss = response.json()["gainloss"]["closed_position"]

        closed_positions = [
            ClosedPosition(
                name=x["symbol"],
                size=x["quantity"],
                cost_basis=x["cost"],
                time_opened=x["open_date"],
                time_closed=x["close_date"],
                proceeds=x["proceeds"],
            )
            for x in gainloss
        ]

        return closed_positions

    async def account_history(self):
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url("/accounts/[[account]]/history/"),
                        params=dict(
                            limit=10000,
                            type=",".join(
                                (
                                    "ach",
                                    "wire",
                                    "dividend",
                                    "fee",
                                    "tax",
                                    "journal",
                                    "check",
                                    "transfer",
                                    "adjustment",
                                    "interest",
                                )
                            ),
                        ),
                        headers=self._headers,
                    )

        if response.status_code != codes.OK:
            raise IOError(
                f"failed to get account history from Tradier "
                f"with a status code of {response.status_code}: {response.text}"
            )

        # actions = [
        #     AccountAction(type=action["type"], amount=action["amount"], date=action["date"])
        #     for action in response.json()["history"]["event"]
        # ]

        # return actions
        return []

    async def calendar(self) -> List[MarketDay]:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url("/markets/calendar/"), headers=self._headers
                    )

        if response.status_code != codes.OK:
            raise IOError(
                f"failed to get market calendar from Tradier "
                f"with a status code of {response.status_code}: {response.text}"
            )

        return None
