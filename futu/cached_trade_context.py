# -*- coding: utf-8 -*-
"""对 Futu 交易上下文的简单缓存包装。

仅为只读型接口（历史成交、现金流、订单费用等）增加本地缓存，不改变原有调用方式。
"""

import logging
import os
from typing import Any, Iterable, List, Optional, Tuple

import pandas as pd
from futu import TrdEnv

from api_cache import ApiCache
from rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class CachedTradeContext:
    """为选定方法增加基于参数的持久化缓存。

    其他未包装的方法会透传到原始 trade_ctx。
    """

    def __init__(
        self,
        trade_ctx: Any,
        cache_dir: Optional[str] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self._trade_ctx = trade_ctx
        base_dir = os.path.dirname(__file__)
        default_dir = os.path.join(base_dir, "..", "data", "api_cache")
        self._cache = ApiCache(cache_dir or default_dir)
        # RateLimiter 仅在实际访问远端接口（缓存未命中）时使用
        self._rate_limiter = rate_limiter

    # --- 包装的只读方法 ---

    def get_acc_cash_flow(self, *, clearing_date: str, acc_id: int) -> Tuple[int, pd.DataFrame]:
        """包装 Futu 的 get_acc_cash_flow 接口，按日期+账户缓存。"""
        method_name = "get_acc_cash_flow"
        params = {
            "clearing_date": clearing_date,
            "acc_id": acc_id,
        }

        def fetch():
            if self._rate_limiter is not None:
                self._rate_limiter.wait_if_needed()
            logger.info(f"[CachedTradeContext] 调用远端: {method_name} {params}")
            return self._trade_ctx.get_acc_cash_flow(
                clearing_date=clearing_date,
                acc_id=acc_id,
            )

        return self._cache.get_or_fetch(method_name, params, fetch)

    def history_deal_list_query(
        self,
        *,
        acc_id: int,
        deal_market: Any,
        start: str,
        end: str,
    ) -> Tuple[int, pd.DataFrame]:
        """包装历史成交查询，按账户+市场+时间窗口缓存。"""
        method_name = "history_deal_list_query"
        params = {
            "acc_id": acc_id,
            "deal_market": str(deal_market),
            "start": start,
            "end": end,
        }

        def fetch():
            if self._rate_limiter is not None:
                self._rate_limiter.wait_if_needed()
            logger.info(f"[CachedTradeContext] 调用远端: {method_name} {params}")
            return self._trade_ctx.history_deal_list_query(
                acc_id=acc_id,
                deal_market=deal_market,
                start=start,
                end=end,
            )

        return self._cache.get_or_fetch(method_name, params, fetch)

    def order_fee_query(
        self,
        *,
        order_id_list: Iterable[str],
        acc_id: int,
        trd_env: TrdEnv,
    ) -> Tuple[int, pd.DataFrame]:
        """包装订单费用查询，按账户+环境+订单ID集合缓存。"""
        method_name = "order_fee_query"
        # 为保证 key 稳定，订单列表排序且转为 list
        order_ids_sorted: List[str] = sorted(order_id_list)
        params = {
            "order_id_list": order_ids_sorted,
            "acc_id": acc_id,
            "trd_env": str(trd_env),
        }

        def fetch():
            if self._rate_limiter is not None:
                self._rate_limiter.wait_if_needed()
            logger.info(f"[CachedTradeContext] 调用远端: {method_name} (订单数={len(order_ids_sorted)})")
            return self._trade_ctx.order_fee_query(
                order_id_list=order_ids_sorted,
                acc_id=acc_id,
                trd_env=trd_env,
            )

        return self._cache.get_or_fetch(method_name, params, fetch)

    # --- 透传其它属性 / 方法 ---

    def __getattr__(self, item: str) -> Any:
        """未显式包装的方法直接透传。"""
        return getattr(self._trade_ctx, item)
