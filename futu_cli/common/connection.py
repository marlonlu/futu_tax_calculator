"""
富途API连接管理模块
提供统一的连接创建和管理功能
"""
import os
import time
import logging
from typing import Tuple, Optional, Dict, Any
from threading import Lock
from futu import OpenQuoteContext, OpenSecTradeContext, TrdMarket, SysConfig, RET_OK


# 配置日志
logger = logging.getLogger(__name__)


class FutuConnectionError(Exception):
    """富途连接异常"""
    pass


class FutuConnectionManager:
    """
    富途连接管理器
    
    提供连接创建和配置验证功能, 不缓存连接
    """
    
    def __init__(self):
        """初始化连接管理器"""
        self.connection_config = None
        self.lock = Lock()
    
    def initialize(self, host: str, port: int, rsa_file: Optional[str] = None, force_remote: Optional[bool] = None):
        """
        初始化连接配置
        
        Args:
            host: 服务器地址
            port: 端口号
            rsa_file: RSA密钥文件路径
            force_remote: 强制指定是否为远程连接
        """
        with self.lock:
            self.connection_config = {
                'host': host,
                'port': port,
                'rsa_file': rsa_file,
                'force_remote': force_remote
            }
            logger.info(f"连接管理器已初始化: {host}:{port}")
    
    def create_connection(self) -> Tuple[OpenQuoteContext, OpenSecTradeContext]:
        """
        创建新连接
        
        Returns:
            (quote_ctx, trade_ctx): 行情上下文和交易上下文
        """
        if not self.connection_config:
            raise FutuConnectionError("连接管理器未初始化")
        
        logger.debug("创建新连接")
        return _create_connection_pair(**self.connection_config)
    
    def close_connection(self, quote_ctx: OpenQuoteContext, trade_ctx: OpenSecTradeContext):
        """
        关闭连接
        
        Args:
            quote_ctx: 行情上下文
            trade_ctx: 交易上下文
        """
        try:
            if quote_ctx:
                quote_ctx.close()
                logger.debug("行情连接已关闭")
        except Exception as e:
            logger.error(f"关闭行情连接时出错: {e}")
        
        try:
            if trade_ctx:
                trade_ctx.close()
                logger.debug("交易连接已关闭")
        except Exception as e:
            logger.error(f"关闭交易连接时出错: {e}")


# 全局连接管理器实例
_connection_manager = FutuConnectionManager()


def create_futu_connections(
    host: Optional[str] = None,
    port: Optional[int] = None,
    rsa_file: Optional[str] = None,
    force_remote: Optional[bool] = None,
    use_manager: bool = False
) -> Tuple[OpenQuoteContext, OpenSecTradeContext]:
    """
    创建富途API连接
    
    Args:
        host: 服务器地址, 默认从环境变量FUTU_ADDRESS获取
        port: 端口号, 默认从环境变量FUTU_PORT获取
        rsa_file: RSA密钥文件路径, 默认从环境变量FUTU_RSA获取
        force_remote: 强制指定是否为远程连接, None时自动判断
        use_manager: 是否使用连接管理器, 默认False保持向后兼容
        
    Returns:
        (quote_ctx, trade_ctx): 行情上下文和交易上下文
        
    Raises:
        FutuConnectionError: 连接创建失败
    """
    # 获取连接参数
    host = host or os.environ.get("FUTU_ADDRESS", "").strip()
    port = port or _get_port_from_env()
    rsa_file = rsa_file or os.environ.get("FUTU_RSA")
    
    # 验证连接配置
    config = _validate_connection_config(host, port, rsa_file, force_remote)
    
    if use_manager:
        # 使用连接管理器
        if not _connection_manager.connection_config:
            _connection_manager.initialize(config['host'], config['port'], config['rsa_file'], config['force_remote'])
        return _connection_manager.create_connection()
    else:
        # 直接创建连接
        return _create_connection_pair(config['host'], config['port'], config['rsa_file'], config['force_remote'])


def _create_connection_pair(
    host: str,
    port: int,
    rsa_file: Optional[str] = None,
    force_remote: Optional[bool] = None
) -> Tuple[OpenQuoteContext, OpenSecTradeContext]:
    """
    创建连接对
    
    Args:
        host: 服务器地址
        port: 端口号
        rsa_file: RSA密钥文件路径
        force_remote: 强制指定是否为远程连接
        
    Returns:
        (quote_ctx, trade_ctx): 行情上下文和交易上下文
    """
    # 判断是否为本地连接
    if force_remote is not None:
        is_local = not force_remote
    else:
        is_local = _is_local_connection(host)
    
    try:
        if is_local:
            logger.info(f"创建本地连接: {host}:{port}")
            return _create_local_connections(host, port)
        else:
            logger.info(f"创建远程连接: {host}:{port}")
            return _create_remote_connections(host, port, rsa_file)
    except Exception as e:
        logger.error(f"创建富途连接失败: {e}")
        raise FutuConnectionError(f"创建富途连接失败: {e}")


def _validate_connection_config(
    host: Optional[str],
    port: Optional[int],
    rsa_file: Optional[str],
    force_remote: Optional[bool]
) -> Dict[str, Any]:
    """
    验证连接配置
    
    Args:
        host: 服务器地址
        port: 端口号
        rsa_file: RSA密钥文件路径
        force_remote: 强制指定是否为远程连接
        
    Returns:
        验证后的配置字典
        
    Raises:
        FutuConnectionError: 配置验证失败
    """
    if not host:
        raise FutuConnectionError("未提供富途服务器地址")
    
    if not port:
        raise FutuConnectionError("未提供富途服务器端口")
    
    # 验证端口范围
    if not (1 <= port <= 65535):
        raise FutuConnectionError(f"端口号超出有效范围: {port}")
    
    # 验证远程连接的RSA文件
    is_remote = force_remote if force_remote is not None else not _is_local_connection(host)
    if is_remote and not rsa_file:
        raise FutuConnectionError("远程连接需要提供RSA密钥文件")
    
    if rsa_file and not os.path.exists(rsa_file):
        raise FutuConnectionError(f"RSA密钥文件不存在: {rsa_file}")
    
    return {
        'host': host,
        'port': port,
        'rsa_file': rsa_file,
        'force_remote': force_remote
    }


def _get_port_from_env() -> Optional[int]:
    """从环境变量获取端口号"""
    port_str = os.environ.get("FUTU_PORT")
    if port_str:
        try:
            return int(port_str)
        except ValueError:
            raise FutuConnectionError(f"无效的端口号: {port_str}")
    return None


def _is_local_connection(host: str) -> bool:
    """判断是否为本地连接"""
    return host == "127.0.0.1"


def _create_local_connections(host: str, port: int) -> Tuple[OpenQuoteContext, OpenSecTradeContext]:
    """创建本地连接"""
    quote_ctx = OpenQuoteContext(host=host, port=port)
    trade_ctx = OpenSecTradeContext(
        host=host, 
        port=port, 
        filter_trdmarket=TrdMarket.NONE, 
        is_encrypt=False
    )
    return quote_ctx, trade_ctx


def _create_remote_connections(
    host: str, 
    port: int, 
    rsa_file: Optional[str]
) -> Tuple[OpenQuoteContext, OpenSecTradeContext]:
    """创建远程连接"""
    if not rsa_file:
        raise FutuConnectionError("远程连接需要提供RSA密钥文件")
    
    # 设置RSA文件
    SysConfig.INIT_RSA_FILE = rsa_file
    
    quote_ctx = OpenQuoteContext(host=host, port=port)
    trade_ctx = OpenSecTradeContext(
        host=host, 
        port=port, 
        filter_trdmarket=TrdMarket.NONE, 
        is_encrypt=True
    )
    return quote_ctx, trade_ctx


def close_connections(quote_ctx: OpenQuoteContext, trade_ctx: OpenSecTradeContext, use_manager: bool = False) -> None:
    """
    安全关闭连接
    
    Args:
        quote_ctx: 行情上下文
        trade_ctx: 交易上下文
        use_manager: 是否使用连接管理器
    """
    if use_manager:
        # 使用连接管理器关闭
        _connection_manager.close_connection(quote_ctx, trade_ctx)
    else:
        # 直接关闭连接
        try:
            if quote_ctx:
                quote_ctx.close()
                logger.debug("行情连接已关闭")
        except Exception as e:
            logger.error(f"关闭行情连接时出错: {e}")
        
        try:
            if trade_ctx:
                trade_ctx.close()
                logger.debug("交易连接已关闭")
        except Exception as e:
            logger.error(f"关闭交易连接时出错: {e}")