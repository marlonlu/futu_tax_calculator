"""
请求频率限制器模块
提供API调用频率控制功能
"""
import time
from collections import deque
from threading import Lock
from typing import Optional


class RateLimiter:
    """
    请求频率限制器
    
    用于控制API调用频率，避免触发富途API的频率限制
    """
    
    def __init__(self, max_requests: int, time_window: int):
        """
        初始化频率限制器
        
        Args:
            max_requests: 时间窗口内最大请求数
            time_window: 时间窗口大小（秒）
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = Lock()
    
    def wait_if_needed(self) -> None:
        """
        如果需要，等待直到可以发送下一个请求
        
        该方法会检查当前请求频率，如果超过限制则等待
        """
        with self.lock:
            now = time.time()
            
            # 移除过期的请求记录
            self._remove_expired_requests(now)
            
            # 如果达到最大请求数，等待
            if len(self.requests) >= self.max_requests:
                wait_time = self._calculate_wait_time(now)
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # 添加新的请求记录
            self.requests.append(now)
    
    def _remove_expired_requests(self, current_time: float) -> None:
        """移除过期的请求记录"""
        while self.requests and current_time - self.requests[0] > self.time_window:
            self.requests.popleft()
    
    def _calculate_wait_time(self, current_time: float) -> float:
        """计算需要等待的时间"""
        if not self.requests:
            return 0
        return self.requests[0] + self.time_window - current_time


def create_rate_limiter(max_requests: int = 9, time_window: int = 30) -> RateLimiter:
    """
    创建默认配置的频率限制器
    
    Args:
        max_requests: 最大请求数，默认9
        time_window: 时间窗口，默认30秒
        
    Returns:
        配置好的RateLimiter实例
    """
    return RateLimiter(max_requests, time_window)