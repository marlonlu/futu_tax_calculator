# -*- coding: utf-8 -*-
"""请求频率限制器模块

提供 RateLimiter 类用于控制 API 请求频率，避免触发接口限制。
"""

import time
from collections import deque
from threading import Lock


class RateLimiter:
    """请求频率限制器
    
    用于控制在指定时间窗口内的最大请求数量，避免触发 API 频率限制。
    线程安全的实现。
    """
    
    def __init__(self, max_requests: int, time_window: int):
        """初始化频率限制器
        
        Args:
            max_requests: 时间窗口内允许的最大请求数
            time_window: 时间窗口长度（秒）
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = Lock()
    
    def wait_if_needed(self) -> None:
        """如果需要则等待，确保不超过频率限制"""
        with self.lock:
            now = time.time()
            # 移除过期的请求记录
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            
            # 如果达到最大请求数，等待
            if len(self.requests) >= self.max_requests:
                wait_time = self.requests[0] + self.time_window - now
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # 添加新的请求记录
            self.requests.append(time.time())