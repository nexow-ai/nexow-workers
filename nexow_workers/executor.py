"""Agent executor — evaluates a single agent and records its signals."""

from __future__ import annotations

from typing import Any

import structlog

from nexow.agents.base import AgentStrategy, Signal, SignalType
from nexow.agents.discretionary import DiscretionaryAgent
from nexow.agents.portfolio import PortfolioAgent
from nexow.agents.systematic import SystematicAgent
from nexow.broker.models import Candle
from nexow.db.client import SupabaseClient

logger = structlog.get_logger(__name__)


class AgentExecutor:
    """
    Evaluates agent strategies and records entry/exit signals.

    Agents are pure signal providers — they do NOT place broker orders.
    Accounting is return-% based: each trade tracks the percentage change
    from entry to exit.
    """

    def __init__(self, db: SupabaseClient) -> None:
        self.db = db

    def _create_strategy(self, agent: dict[str, Any]) -> AgentStrategy:
        """Instantiate the correct strategy class based on agent type."""
        agent_id = agent["id"]
        config = agent.get("config", {})
        agent_type = agent.get("type", "systematic")

        if agent_type == "discretionary":
            return DiscretionaryAgent(agent_id, config)
        return SystematicAgent(agent_id, config)

    async def execute(
        self,
        agent: dict[str, Any],
        candles: list[Candle],
        current_price: float,
    ) -> None:
        """
        Run one evaluation cycle for a single agent on one instrument.

        1. Create strategy instance
        2. Evaluate candles → Signal
        3. BUY/SELL → record entry (if no open trade for this instrument)
        4. CLOSE → close open trades with return_%
        5. HOLD → do nothing
        """
        agent_id = agent["id"]
        instrument = candles[0].instrument if candles else agent.get("instrument", "EUR_USD")

        try:
            strategy = self._create_strategy(agent)
            signal: Signal = await strategy.evaluate(candles, current_price)

            logger.info(
                "agent_signal",
                agent_id=agent_id[:8],
                name=agent.get("name", "?"),
                instrument=instrument,
                signal=signal.type.value,
                reason=signal.reason,
                confidence=f"{signal.confidence:.2f}",
            )

            if signal.type == SignalType.HOLD:
                return

            if signal.type == SignalType.CLOSE:
                self._close_positions(agent_id, instrument, current_price)
                return

            # BUY or SELL — open a new trade (one per instrument per agent)
            open_trades = self.db.get_open_trades(agent_id)
            already_open = any(t["instrument"] == instrument for t in open_trades)
            if already_open:
                logger.debug(
                    "trade_skipped_already_open",
                    agent_id=agent_id[:8],
                    instrument=instrument,
                )
                return

            trade_record: dict[str, Any] = {
                "agent_id": agent_id,
                "instrument": instrument,
                "direction": signal.type.value,
                "entry_price": current_price,
                "status": "open",
                "stop_loss_pct": signal.stop_loss_pct,
                "take_profit_pct": signal.take_profit_pct,
            }
            self.db.insert_trade(trade_record)

            logger.info(
                "signal_recorded",
                agent_id=agent_id[:8],
                instrument=instrument,
                direction=signal.type.value,
                entry_price=current_price,
                sl_pct=signal.stop_loss_pct,
                tp_pct=signal.take_profit_pct,
            )

        except Exception as e:
            logger.error("agent_execution_error", agent_id=agent_id, instrument=instrument, error=str(e))

    def _close_positions(
        self,
        agent_id: str,
        instrument: str,
        current_price: float,
    ) -> None:
        """Close all open trades for an agent on a specific instrument."""
        open_trades = self.db.get_open_trades(agent_id)

        for trade in open_trades:
            if trade.get("instrument") != instrument:
                continue

            entry = float(trade["entry_price"])
            direction = trade["direction"]

            # Calculate gross return %
            if direction == "buy":
                return_pct = ((current_price - entry) / entry) * 100
            else:
                return_pct = ((entry - current_price) / entry) * 100

            self.db.close_trade(trade["id"], current_price, return_pct)

        logger.info("positions_closed", agent_id=agent_id[:8], instrument=instrument, price=current_price)
