"""Main worker loop — polls agents every tick and evaluates them."""

from __future__ import annotations

import asyncio

import structlog

from nexow.ai.factory import generate_strategy
from nexow.agents.portfolio import PortfolioAgent
from nexow.broker.oanda import OandaClient
from nexow.config import settings
from nexow.db.client import SupabaseClient
from nexow.worker.executor import AgentExecutor

logger = structlog.get_logger(__name__)


class WorkerLoop:
    """
    The main engine loop.

    Every tick:
    1. Process pending agents (generate config from prompt)
    2. Sync open trades (check %-based SL/TP against live prices)
    3. Fetch all active agents
    4. For each agent, evaluate each instrument and record signals
    """

    def __init__(self) -> None:
        self.db = SupabaseClient()
        self.market = OandaClient()
        self.executor = AgentExecutor(self.db)
        self._running = False

    async def start(self) -> None:
        """Start the infinite worker loop."""
        self._running = True
        logger.info("worker_started", tick_interval=settings.tick_interval_seconds)

        while self._running:
            try:
                await self._tick()
            except Exception as e:
                logger.error("tick_error", error=str(e))

            await asyncio.sleep(settings.tick_interval_seconds)

    async def stop(self) -> None:
        """Gracefully stop the worker loop."""
        logger.info("worker_stopping")
        self._running = False
        await self.market.close()

    async def _process_pending(self) -> None:
        """Check for newly created agents that need AI config generation."""
        try:
            pending = self.db.get_pending_agents()
        except Exception:
            return

        for agent in pending:
            prompt = agent.get("prompt", "")
            if not prompt:
                continue

            logger.info("generating_agent_config", agent_id=agent["id"], prompt=prompt[:80])
            try:
                result = await generate_strategy(prompt)

                config = result.config
                portfolio = config.get("portfolio", {})
                instruments = portfolio.get("instruments", [])

                update_data: dict = {
                    "name": result.name,
                    "description": result.description,
                    "type": result.agent_type.value if hasattr(result.agent_type, "value") else result.agent_type,
                    "config": config,
                    "status": "active",
                }

                if instruments:
                    update_data["instruments"] = instruments
                    update_data["instrument"] = instruments[0].get("instrument", "EUR_USD")
                    update_data["timeframe"] = instruments[0].get("timeframe", "M5")

                self.db.update_agent_config(agent["id"], config)
                self.db.client.table("agents").update(update_data).eq("id", agent["id"]).execute()
                logger.info("agent_activated", agent_id=agent["id"], name=result.name)
            except Exception as e:
                logger.error("agent_generation_failed", agent_id=agent["id"], error=str(e))

    async def _sync_trades(self) -> None:
        """
        Check percentage-based SL/TP for all open trades.

        For each open trade that has a stop_loss_pct or take_profit_pct,
        fetch the current price and close the trade if the level was hit.
        """
        try:
            open_trades = self.db.get_all_open_trades()
            if not open_trades:
                return

            # Group by instrument to minimise price fetches
            by_instrument: dict[str, list[dict]] = {}
            for trade in open_trades:
                inst = trade["instrument"]
                by_instrument.setdefault(inst, []).append(trade)

            for inst, trades in by_instrument.items():
                try:
                    price = await self.market.get_price(inst)
                except Exception:
                    continue

                for trade in trades:
                    sl_pct = float(trade["stop_loss_pct"]) if trade.get("stop_loss_pct") else None
                    tp_pct = float(trade["take_profit_pct"]) if trade.get("take_profit_pct") else None

                    if sl_pct is None and tp_pct is None:
                        continue

                    entry = float(trade["entry_price"])
                    direction = trade["direction"]

                    # Calculate current return %
                    if direction == "buy":
                        current_return = ((price - entry) / entry) * 100
                    else:
                        current_return = ((entry - price) / entry) * 100

                    exit_triggered = False
                    exit_reason = ""

                    # Stop-loss: return drops below -sl_pct
                    if sl_pct is not None and current_return <= -sl_pct:
                        exit_triggered = True
                        exit_reason = "SL"

                    # Take-profit: return rises above +tp_pct
                    if tp_pct is not None and current_return >= tp_pct:
                        exit_triggered = True
                        exit_reason = "TP"

                    if exit_triggered:
                        self.db.close_trade(trade["id"], price, current_return)
                        logger.info(
                            "trade_sl_tp_hit",
                            trade_id=trade["id"][:8],
                            instrument=inst,
                            exit_price=price,
                            return_pct=round(current_return, 4),
                            reason=exit_reason,
                        )

        except Exception as e:
            logger.debug("trade_sync_error", error=str(e))

    async def _tick(self) -> None:
        """Execute one tick of the worker loop."""
        await self._process_pending()
        await self._sync_trades()

        agents = self.db.get_active_agents()
        if not agents:
            return

        logger.debug("tick", active_agents=len(agents))

        for agent in agents:
            try:
                portfolio = PortfolioAgent(agent)

                for inst_config in portfolio.instruments_config:
                    instrument = inst_config["instrument"]
                    timeframe = inst_config.get("timeframe", "M5")

                    schedule = agent.get("evaluation_schedule", "every_tick")
                    if schedule != "every_tick" and agent.get("type") == "discretionary":
                        continue

                    candles = await self.market.get_candles(
                        instrument=instrument,
                        granularity=timeframe,
                    )
                    current_price = await self.market.get_price(instrument)

                    await self.executor.execute(agent, candles, current_price)

            except Exception as e:
                logger.error(
                    "agent_processing_error",
                    agent_id=agent.get("id", "?"),
                    error=str(e),
                )
