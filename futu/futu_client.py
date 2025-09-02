# -*- coding: utf-8 -*-
"""Futu API 客户端管理模块

提供 FutuClient 类用于管理 Futu API 连接和账户操作。
"""

import os
from typing import Tuple

import pandas as pd
from dotenv import load_dotenv
from futu import *


class FutuClient:
    """Futu API 客户端管理类
    
    负责管理 Futu API 的连接创建和账户相关操作。
    """
    
    def __init__(self):
        """初始化 Futu 客户端"""
        # 加载环境变量
        load_dotenv()
        self.quote_ctx = None
        self.trade_ctx = None
    
    def create_connections(self) -> Tuple[OpenQuoteContext, OpenSecTradeContext]:
        """创建 Futu API 连接
        
        Returns:
            Tuple[OpenQuoteContext, OpenSecTradeContext]: (quote_ctx, trade_ctx) 连接对象元组
        """
        host = os.environ.get("FUTU_ADDRESS", "").strip()
        port = int(os.environ.get("FUTU_PORT"))
        is_local_futu_api = host == "127.0.0.1"
        
        if is_local_futu_api:
            # 创建OpenD连接
            quote_ctx = OpenQuoteContext(host=host, port=port)
            # 不指定市场，获取所有市场的交易权限
            trade_ctx = OpenSecTradeContext(
                host=host, 
                port=port, 
                filter_trdmarket=TrdMarket.NONE, 
                is_encrypt=False
            )
        else:
            # 不是本地网络请求必须要设置 rsa
            SysConfig.INIT_RSA_FILE = os.environ.get("FUTU_RSA")
            # 创建OpenD连接
            quote_ctx = OpenQuoteContext(host=host, port=port)
            # 不指定市场，获取所有市场的交易权限
            trade_ctx = OpenSecTradeContext(
                host=host, 
                port=port, 
                filter_trdmarket=TrdMarket.NONE, 
                is_encrypt=True
            )
        
        self.quote_ctx = quote_ctx
        self.trade_ctx = trade_ctx
        return quote_ctx, trade_ctx
    
    def get_valid_accounts(self, trade_ctx: OpenSecTradeContext) -> pd.DataFrame:
        """获取有效的账户列表，过滤掉模拟账户和现金账户
        
        Args:
            trade_ctx: 交易上下文对象
            
        Returns:
            pd.DataFrame: 有效的账户列表
            
        Raises:
            Exception: 当获取账户列表失败时抛出异常
        """
        ret, acc_list_df = trade_ctx.get_acc_list()
        if ret != RET_OK or not isinstance(acc_list_df, pd.DataFrame):
            raise Exception(f'获取账户列表失败: {acc_list_df}')
        
        # 过滤有效账户：排除模拟账户和现金账户，排除无效acc_id
        valid_accounts = acc_list_df[
            (acc_list_df.get("trd_env") != TrdEnv.SIMULATE) &
            (acc_list_df.get("acc_type") != TrdAccType.CASH) &
            (acc_list_df['acc_id'].notna())
        ].copy()
        
        return valid_accounts
    
    def close_connections(self) -> None:
        """关闭连接"""
        if self.quote_ctx:
            self.quote_ctx.close()
        if self.trade_ctx:
            self.trade_ctx.close()