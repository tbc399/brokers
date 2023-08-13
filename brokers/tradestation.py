from broker import Broker, OrderType, AccountBalance, Order
from typing import Collection
from tenacity import AsyncRetrying, stop_after_attempt
import httpx

_order_type_map = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
}


class TradeStation(Broker):
    def __init__(self, account_number: str, **kwargs):
        super().__init__(account_number)

        access_token = kwargs.get("access_token")
        if access_token is None:
            raise ValueError("must have an access token to instantiate Tradestation broker")

        self._api_url = kwargs.get("url", "api.tradestation.com").strip("/")
        self._api_version = kwargs.get("version", "v3").strip("/")

        self._headers = dict(Authorization=f"Bearer {access_token}")

    def _build_url(self, path):
        return f"https://{self._api_url}/{self._api_version}/{path.strip('/')}"

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
                        headers=self._headers,
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
        raise NotImplementedError()

    async def get_quotes(self, names: Collection[str]) -> List[Quote]:
        raise NotImplementedError()

    @property
    async def account_balance(self) -> AccountBalance:
        async with httpx.AsyncClient() as client:
            async for attempt in AsyncRetrying(stop=stop_after_attempt(4)):
                with attempt:
                    response = await client.get(
                        url=self._build_url(f"brokerage/accounts/{self._account_number}/balances"),
                        headers=self._headers,
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

    @property
    async def orders(self) -> Collection[Order]:
        raise NotImplementedError()

    async def cancel_order(self, order_id):
        raise NotImplementedError()

    @property
    async def account_pnl(self) -> ReturnStream:
        raise NotImplementedError()

    async def account_history(self):
        raise NotImplementedError()

    async def calendar(self) -> List[MarketDay]:
        raise NotImplementedError()
