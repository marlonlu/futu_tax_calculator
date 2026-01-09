# -*- coding: utf-8 -*-
"""Futu API 请求结果缓存模块。

为只读类接口提供基于「方法名 + 参数」的本地持久化缓存，避免重复请求。
"""

import hashlib
import json
import logging
import os
import pickle
from typing import Any, Callable, Dict, Tuple

logger = logging.getLogger(__name__)


class ApiCache:
    """简易文件缓存：key -> pickle 序列化结果。"""

    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    @staticmethod
    def _normalize_params(params: Dict[str, Any]) -> str:
        """将参数规范化为可哈希的字符串（用于生成缓存 key）。"""
        # 只接受简单 dict，复杂对象统一走 default=str
        try:
            payload = json.dumps(
                params,
                sort_keys=True,
                default=str,
                ensure_ascii=False,
            )
        except TypeError:
            # 回退方案：使用 repr，保证至少是稳定字符串
            payload = repr(sorted(params.items()))
        return payload

    def _build_cache_path(self, method_name: str, params: Dict[str, Any]) -> str:
        normalized = self._normalize_params(params)
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        method_dir = os.path.join(self.cache_dir, method_name)
        os.makedirs(method_dir, exist_ok=True)
        return os.path.join(method_dir, f"{digest}.pkl")

    def load(self, method_name: str, params: Dict[str, Any]) -> Tuple[bool, Any]:
        """尝试读取缓存。

        Returns:
            (hit, value)
        """
        path = self._build_cache_path(method_name, params)
        if not os.path.exists(path):
            return False, None
        try:
            with open(path, "rb") as f:
                value = pickle.load(f)
            logger.info(f"[ApiCache] 命中缓存: {method_name}")
            return True, value
        except Exception as e:
            logger.warning(f"[ApiCache] 读取缓存失败，将忽略该缓存。原因: {e}")
            return False, None

    def save(self, method_name: str, params: Dict[str, Any], value: Any) -> None:
        """写入缓存。"""
        path = self._build_cache_path(method_name, params)
        try:
            with open(path, "wb") as f:
                pickle.dump(value, f)
            logger.info(f"[ApiCache] 已写入缓存: {method_name} -> {os.path.basename(path)}")
        except Exception as e:
            logger.warning(f"[ApiCache] 写入缓存失败，忽略。原因: {e}")

    def get_or_fetch(
        self,
        method_name: str,
        params: Dict[str, Any],
        fetcher: Callable[[], Any],
    ) -> Any:
        """命中则直接返回缓存，否则调用 fetcher 并根据返回结果决定是否缓存。"""
        hit, value = self.load(method_name, params)
        if hit:
            return value

        result = fetcher()

        # 针对 Futu 的接口约定：返回 (ret, data)
        try:
            if isinstance(result, tuple) and len(result) >= 2:
                ret_code = result[0]
                from futu import RET_OK  # 延迟导入，避免循环依赖

                if ret_code == RET_OK:
                    self.save(method_name, params, result)
            else:
                # 其他情况也允许写缓存（例如未来扩展）
                self.save(method_name, params, result)
        except Exception as e:
            logger.warning(f"[ApiCache] 决定是否缓存时出错，将不缓存本次结果。原因: {e}")

        return result

