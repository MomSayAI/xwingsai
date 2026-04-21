# File: src/config/encrypt_config.py
# Purpose: Encrypt configuration files for xWings trading system
# Dependencies: os, yaml, cryptography.fernet, logging, pathlib
# Notes: Uses PathManager for path management. Timestamps in UTC+8.
#        Enhanced with batch verification, robust logging, and decryption validation.
#        Updated to work with client_credentials.yaml, markets.yaml, parameters.yaml.
#        Removed environment-specific logic (test/prod).
# Updated: 2025-07-03 13:30 PDT

import sys
import os
from pathlib import Path
import yaml
import logging
from typing import Dict
from cryptography.fernet import Fernet

# Add xWings/ to sys.path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # xWings/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.utils import PathManager
from src.core.logging_config import configure_logging

def generate_key() -> bytes:
    """Generate a new 44-byte (32-byte decoded) encryption key."""
    key = Fernet.generate_key()
    if len(key) != 44:
        raise ValueError(f"Generated key length {len(key)} does not match required 44 bytes (base64-encoded)")
    return key

def encrypt_file(input_path: Path, output_path: Path, key: bytes, logger: logging.Logger) -> None:
    """
    Encrypt a YAML file and verify the result.
    
    Args:
        input_path (Path): Path to input YAML file
        output_path (Path): Path to output encrypted file
        key (bytes): Encryption key
        logger (logging.Logger): Logger instance
    """
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    try:
        with input_path.open('r', encoding='utf-8') as f:
            data = f.read()
        if not data:
            logger.error(f"Input file {input_path} is empty")
            raise ValueError(f"Input file {input_path} is empty")
        
        yaml_data = yaml.safe_load(data)  # Validate YAML structure
        fernet = Fernet(key)
        encrypted_data = fernet.encrypt(data.encode('utf-8'))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('wb') as f:
            f.write(encrypted_data)
        
        # Verify decryption
        with output_path.open('rb') as f:
            decrypted_data = fernet.decrypt(f.read()).decode('utf-8')
        decrypted_yaml = yaml.safe_load(decrypted_data)
        if decrypted_yaml != yaml_data:
            logger.error(f"Encryption verification failed for {input_path}: decrypted data does not match input")
            raise RuntimeError(f"Encryption verification failed for {input_path}")
        
        logger.info(f"Successfully encrypted {input_path} to {output_path} with key length {len(key)}")
    except (yaml.YAMLError, yaml.scanner.ScannerError) as e:
        logger.error(f"Invalid YAML in {input_path}: {str(e)}. Skipping this file.")
        raise
    except Exception as e:
        logger.error(f"Encryption error for {input_path}: {str(e)}")
        raise

def decrypt_yaml(input_path: Path, key_file: Path) -> Dict:
    """
    Decrypt a YAML file and return its contents.

    Args:
        input_path (Path): Path to encrypted YAML file
        key_file (Path): Path to encryption key file

    Returns:
        Dict: Decrypted YAML content

    Raises:
        FileNotFoundError: If input or key file is missing
        RuntimeError: If decryption fails
    """
    logger = logging.getLogger(__name__)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not key_file.exists():
        logger.error(f"Key file not found: {key_file}")
        raise FileNotFoundError(f"Key file not found: {key_file}")
    
    try:
        with key_file.open('rb') as f:
            key = f.read()
        if len(key) != 44:
            logger.error(f"Invalid key length: {len(key)} bytes, expected 44 (base64-encoded)")
            raise ValueError(f"Invalid key length: {len(key)} bytes, expected 44 (base64-encoded)")
        fernet = Fernet(key)
        with input_path.open('rb') as f:
            decrypted_data = fernet.decrypt(f.read()).decode('utf-8')
        data = yaml.safe_load(decrypted_data)
        if data is None:
            logger.error(f"Decrypted YAML from {input_path} is empty or invalid")
            raise RuntimeError(f"Decrypted YAML from {input_path} is empty or invalid")
        logger.debug(f"Successfully decrypted {input_path} with key length {len(key)}")
        return data
    except Exception as e:
        logger.error(f"Decryption error for {input_path}: {str(e)}")
        raise RuntimeError(f"Decryption error for {input_path}: {str(e)}")

def verify_decryption(input_path: Path, encrypted_path: Path, key_file: Path, logger: logging.Logger) -> None:
    """
    Verify that encrypted file matches the input YAML file after decryption.

    Args:
        input_path (Path): Path to original YAML file
        encrypted_path (Path): Path to encrypted YAML file
        key_file (Path): Path to encryption key file
        logger (logging.Logger): Logger instance
    """
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not encrypted_path.exists():
        logger.error(f"Encrypted file not found: {encrypted_path}")
        raise FileNotFoundError(f"Encrypted file not found: {encrypted_path}")
    
    try:
        with input_path.open('r', encoding='utf-8') as f:
            original_data = yaml.safe_load(f.read())
        decrypted_data = decrypt_yaml(encrypted_path, key_file)
        if original_data != decrypted_data:
            logger.error(f"Decryption mismatch for {encrypted_path}: decrypted data does not match {input_path}")
            raise RuntimeError(f"Decryption mismatch for {encrypted_path}")
        logger.info(f"Verified decryption for {encrypted_path} matches {input_path} with key length {len(Path(key_file).read_bytes())}")
    except Exception as e:
        logger.error(f"Verification error for {encrypted_path}: {str(e)}")
        raise

def encrypt_configs(config_dir: Path, key_file: Path, logger: logging.Logger) -> None:
    """
    Encrypt all unencrypted YAML files in the config directory.

    Args:
        config_dir (Path): Directory containing YAML files
        key_file (Path): Path to encryption key file
        logger (logging.Logger): Logger instance
    """
    try:
        if not key_file.exists():
            key = generate_key()
            key_file.parent.mkdir(parents=True, exist_ok=True)
            with key_file.open('wb') as f:
                f.write(key)
            logger.info(f"Generated and saved new key to {key_file} with length {len(key)}")
        else:
            with key_file.open('rb') as f:
                key = f.read()
            if len(key) != 44:
                logger.error(f"Existing key length {len(key)} does not match required 44 bytes (base64-encoded), regenerating")
                key = generate_key()
                with key_file.open('wb') as f:
                    f.write(key)
                logger.info(f"Regenerated and saved new key to {key_file} with length {len(key)}")
            else:
                logger.info(f"Using existing key from {key_file} with length {len(key)}")

        all_files = os.listdir(config_dir)
        yaml_files = [
            f for f in all_files
            if f.endswith('.yaml') and not f.endswith('_encrypted.yaml') and f != 'encryption.key'
            and f in ['client_credentials.yaml', 'markets.yaml', 'parameters.yaml']
        ]
        if not yaml_files:
            logger.warning(f"No unencrypted .yaml files found in {config_dir} to encrypt")
            return

        for yaml_file in yaml_files:
            input_path = config_dir / yaml_file
            output_path = config_dir / f"{Path(yaml_file).stem}_encrypted.yaml"
            if output_path.exists():
                logger.info(f"Skipping {input_path} as encrypted version {output_path} already exists")
                continue
            logger.debug(f"Processing: {input_path}")
            encrypt_file(input_path, output_path, key, logger)
            verify_decryption(input_path, output_path, key_file, logger)
    except Exception as e:
        logger.error(f"Batch encryption error: {str(e)}")
        raise

def main():
    """Main function to encrypt all unencrypted YAML files in the config directory."""
    try:
        path_manager = PathManager(PROJECT_ROOT)
        config_dir = path_manager.config_path
        log_file = path_manager.get_log_path('encrypt_config.log')
        logger = configure_logging(log_file=str(log_file))
        
        key_file = config_dir / 'encryption.key'
        encrypt_configs(config_dir, key_file, logger)
    except Exception as e:
        logger.error(f"Main encryption error: {str(e)}")
        raise

if __name__ == '__main__':
    main()