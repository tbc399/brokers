from broker import Broker


def _convert_alpaca_order_status(order_status: AlpacaOrderStatus) -> OrderStatus:
    match order_status:
        case AlpacaOrderStatus.NEW:
            return OrderStatus("open")
        case _:
            return OrderStatus(order_status.value)


class Alpaca(Broker):
    def __init__(self):
        super().__init__("")

        api_key = environ.get("ALPACA_API_KEY")
        secret = environ.get("ALPACA_SECRET_KEY")

        self.client = TradingClient(api_key=api_key, secret_key=secret, paper=True)
        self.market_client = StockHistoricalDataClient(
            api_key=api_key, secret_key=secret
        )

        self._account_number = self.client.get_account().account_number

    async def _place_order(
        self,
        name: str,
        quantity: int,
        side: str,
        order_type: str = "market",
        stop_price: float = None,
    ) -> str:
        try:
            order = self.client.submit_order(
                MarketOrderRequest(
                    symbol=name,
                    qty=quantity,
                    side=side,
                    time_in_force=TimeInForce.GTC,
                )
            )
        except APIError as e:
            raise IOError(
                f"failed to place market {side} order from Alpaca "
                f"for {name} with error: {e}"
            )

        return str(order.id)

    async def place_market_sell(self, name: str, quantity: int) -> str:
        return await self._place_order(name, quantity, "sell")

    async def place_market_buy(self, name: str, quantity: int) -> str:
        return await self._place_order(name, quantity, "buy")

    async def place_stop_loss(self, name: str, quantity: int, price: float):
        return await self._place_order(name, quantity, "sell", "stop", price)

    @property
    async def account_balance(self) -> AccountBalance:
        account = self.client.get_account()
        return AccountBalance(
            total_cash=float(account.cash),
            total_equity=float(account.equity),
            open_pl=0,
            long_value=float(account.long_market_value),
            settled_cash=float(account.cash) - float(account.pending_transfer_in),
        )

    @property
    async def positions(self) -> List[Position]:
        return [
            Position(
                name=pos.symbol,
                size=pos.qty,
                cost_basis=pos.cost_basis,
                time_opened=datetime.utcnow(),  #  FIXME: this is just to fill in for now
            )
            for pos in self.client.get_all_positions()
        ]

    async def get_quote(self, name: str) -> Quote:
        quote = self.market_client.get_stock_latest_quote(
            request_params=StockLatestQuoteRequest(symbol_or_symbols=name)
        )
        return Quote(name=name, price=quote[name].ask_price)

    async def get_quotes(self, names: Collection[str]) -> List[Quote]:
        raise NotImplementedError()

    async def order_status(self, order_id: str) -> Order:
        order = self.client.get_order_by_id(order_id)
        return Order(
            id=order_id,
            name=order.symbol,
            side=order.side,
            type=order.type,
            status=_convert_alpaca_order_status(order.status),
            executed_quantity=int(float(order.filled_qty)),
            avg_fill_price=float(order.filled_avg_price or "0.0"),
        )

    @property
    async def orders(self) -> Collection[Order]:
        self.orders_ = [
            Order(
                id=str(order.id),
                name=order.symbol,
                side=order.side,
                type=order.type,
                status=_convert_alpaca_order_status(order.status),
                executed_quantity=int(float(order.filled_qty)),
                avg_fill_price=float(order.filled_avg_price or 0.0),
            )
            for order in self.client.get_orders()
        ]
        return self.orders_

    async def cancel_order(self, order_id):
        return NotImplementedError()

    async def account_pnl(self, since_date: date = None) -> List[ClosedPosition]:
        raise NotImplementedError()

    async def account_history(self):
        raise NotImplementedError()

    async def calendar(self) -> List[MarketDay]:
        raise NotImplementedError()
