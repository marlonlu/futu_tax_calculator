"""
配置管理模块
提供统一的配置加载和管理功能
"""
import os
import yaml
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """配置异常"""
    pass


class ConfigManager:
    """
    配置管理器
    
    提供配置文件加载、缓存和环境变量覆盖功能
    """
    
    def __init__(self, config_dir: str = "config"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录，默认为 "config"
        """
        self.config_dir = Path(config_dir)
        self._cache = {}
        
        # 确保配置目录存在
        if not self.config_dir.exists():
            logger.warning(f"配置目录不存在: {self.config_dir}")
    
    def load_config(self, config_name: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        加载配置文件
        
        Args:
            config_name: 配置文件名（不含扩展名）
            use_cache: 是否使用缓存，默认True
            
        Returns:
            配置字典
            
        Raises:
            ConfigError: 配置加载失败
        """
        # 检查缓存
        if use_cache and config_name in self._cache:
            logger.debug(f"从缓存加载配置: {config_name}")
            return self._cache[config_name].copy()
        
        # 构建配置文件路径
        config_file = self.config_dir / f"{config_name}.yaml"
        
        if not config_file.exists():
            raise ConfigError(f"配置文件不存在: {config_file}")
        
        try:
            # 加载YAML配置
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f) or {}
            
            logger.info(f"成功加载配置文件: {config_file}")
            
            # 应用环境变量覆盖
            config_data = self._apply_env_overrides(config_data, config_name)
            
            # 缓存配置
            if use_cache:
                self._cache[config_name] = config_data.copy()
            
            return config_data
            
        except yaml.YAMLError as e:
            raise ConfigError(f"解析YAML配置文件失败 {config_file}: {e}")
        except Exception as e:
            raise ConfigError(f"加载配置文件失败 {config_file}: {e}")
    
    def get_config_value(
        self, 
        config_name: str, 
        key_path: str, 
        default: Any = None
    ) -> Any:
        """
        获取配置值
        
        Args:
            config_name: 配置文件名
            key_path: 配置键路径，支持点分隔的嵌套路径，如 "database.host"
            default: 默认值
            
        Returns:
            配置值
        """
        try:
            config = self.load_config(config_name)
            return self._get_nested_value(config, key_path, default)
        except ConfigError:
            logger.warning(f"获取配置值失败: {config_name}.{key_path}，使用默认值")
            return default
    
    def _get_nested_value(self, data: Dict[str, Any], key_path: str, default: Any) -> Any:
        """
        获取嵌套字典中的值
        
        Args:
            data: 数据字典
            key_path: 键路径，如 "a.b.c"
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key_path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current
    
    def _apply_env_overrides(self, config: Dict[str, Any], config_name: str) -> Dict[str, Any]:
        """
        应用环境变量覆盖
        
        Args:
            config: 原始配置
            config_name: 配置名称
            
        Returns:
            应用环境变量后的配置
        """
        # 连接配置的特殊处理
        if config_name == "connection_config":
            connection_config = config.get("connection", {})
            
            # 环境变量优先级高于配置文件
            if "FUTU_ADDRESS" in os.environ:
                connection_config["default_host"] = os.environ["FUTU_ADDRESS"]
            
            if "FUTU_PORT" in os.environ:
                try:
                    connection_config["default_port"] = int(os.environ["FUTU_PORT"])
                except ValueError:
                    logger.warning(f"无效的FUTU_PORT环境变量: {os.environ['FUTU_PORT']}")
            
            config["connection"] = connection_config
        
        return config
    
    def clear_cache(self, config_name: Optional[str] = None) -> None:
        """
        清除配置缓存
        
        Args:
            config_name: 要清除的配置名称，None表示清除所有缓存
        """
        if config_name:
            self._cache.pop(config_name, None)
            logger.debug(f"清除配置缓存: {config_name}")
        else:
            self._cache.clear()
            logger.debug("清除所有配置缓存")
    
    def reload_config(self, config_name: str) -> Dict[str, Any]:
        """
        重新加载配置文件
        
        Args:
            config_name: 配置文件名
            
        Returns:
            重新加载的配置字典
        """
        self.clear_cache(config_name)
        return self.load_config(config_name)


# 全局配置管理器实例
_config_manager = ConfigManager()


def load_config(config_name: str) -> Dict[str, Any]:
    """
    加载指定配置文件
    
    Args:
        config_name: 配置文件名（不含扩展名）
        
    Returns:
        配置字典
        
    Raises:
        ConfigError: 配置加载失败
    """
    return _config_manager.load_config(config_name)


def get_config_value(config_name: str, key_path: str, default: Any = None) -> Any:
    """
    获取配置值
    
    Args:
        config_name: 配置文件名
        key_path: 配置键路径，支持点分隔的嵌套路径
        default: 默认值
        
    Returns:
        配置值
    """
    return _config_manager.get_config_value(config_name, key_path, default)


def clear_config_cache(config_name: Optional[str] = None) -> None:
    """
    清除配置缓存
    
    Args:
        config_name: 要清除的配置名称，None表示清除所有缓存
    """
    _config_manager.clear_cache(config_name)


def reload_config(config_name: str) -> Dict[str, Any]:
    """
    重新加载配置文件
    
    Args:
        config_name: 配置文件名
        
    Returns:
        重新加载的配置字典
    """
    return _config_manager.reload_config(config_name)