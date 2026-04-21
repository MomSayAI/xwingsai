# File: decrypt_configs.py
import sys
import os
from pathlib import Path
import yaml
import logging
from encrypt_config import decrypt_yaml

# 设置项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # xWings/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# 配置日志
def setup_logger():
    logger = logging.getLogger("decrypt_config")
    logger.setLevel(logging.INFO)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 创建文件处理器
    log_path = PROJECT_ROOT / "logs" / "decrypt_config.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)

    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # 添加处理器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def decrypt_all_configs(config_dir: Path, key_file: Path, logger: logging.Logger):
    """
    解密所有加密配置文件
    """
    if not config_dir.exists():
        logger.error(f"配置目录不存在: {config_dir}")
        return

    if not key_file.exists():
        logger.error(f"密钥文件不存在: {key_file}")
        return

    encrypted_files = [
        f for f in config_dir.iterdir()
        if f.is_file() and f.suffix == ".yaml" and "_encrypted" in f.stem
    ]

    if not encrypted_files:
        logger.info(f"在 {config_dir} 中没有找到加密的配置文件")
        return

    for encrypted_file in encrypted_files:
        try:
            # 创建解密文件名（移除 "_encrypted"）
            decrypted_filename = encrypted_file.stem.replace("_encrypted", "") + ".yaml"
            decrypted_path = config_dir / decrypted_filename

            # 解密文件
            data = decrypt_yaml(encrypted_file, key_file)

            # 保存解密后的文件
            with decrypted_path.open("w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

            logger.info(f"成功解密 {encrypted_file.name} 到 {decrypted_filename}")

        except Exception as e:
            logger.error(f"解密 {encrypted_file.name} 失败: {str(e)}")


if __name__ == "__main__":
    logger = setup_logger()

    # 设置路径
    config_dir = PROJECT_ROOT / "src"/"config"
    key_file = config_dir / "encryption.key"

    # 解密所有配置文件
    decrypt_all_configs(config_dir, key_file, logger)
    logger.info("解密过程完成")