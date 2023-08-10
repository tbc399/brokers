from broker import Broker
from tenacity import AsyncRetrying, stop_after_attempt
import httpx


class TradeStation(Broker):
    def __init__(self, account_number: str, **kwargs):
        super().__init__(account_number)

        access_token = kwargs.get("access_token")
        if access_token is None:
            raise ValueError(
                "must have an access token to instantiate Tradestation broker"
            )

        self._api_url = kwargs.get("url", "api.tradestation.com").strip("/")
        self._api_version = kwargs.get("version", "v3").strip("/")

        self._headers = dict(
            Accept="application/json", Authorization=f"Bearer {access_token}"
        )

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

        if response.status_code != httpx.codes.OK:
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

    async def get_quote(self, name: str) -> Quote:
        pass

    async def get_quotes(self, names: Collection[str]) -> List[Quote]:
        pass

    @property
    async def account_balance(self) -> AccountBalance:
        pass

    @property
    async def orders(self) -> Collection[Order]:
        pass

    async def cancel_order(self, order_id):
        pass

    @property
    async def account_pnl(self) -> ReturnStream:
        pass

    async def account_history(self):
        pass

    @abstractmethod
    async def calendar(self) -> List[MarketDay]:
        pass
