import os
import json
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class PromptBackupManager:
    """Manages file-based backups of prompts with automatic versioning"""

    def __init__(self, base_backup_dir: str = "/app/prompt_backups"):
        self.base_backup_dir = Path(base_backup_dir)
        self.base_backup_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Prompt backup directory: {self.base_backup_dir}")

    def _sanitize_filename(self, text: str) -> str:
        """Sanitize text for use in filename"""
        # Remove or replace characters that aren't safe for filenames
        safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        sanitized = "".join(c if c in safe_chars else "_" for c in text)
        # Limit length
        return sanitized[:100]

    def _get_prompt_directory(self, username: str, prompt_type: str, prompt_name: str) -> Path:
        """Get the directory path for a specific prompt"""
        safe_username = self._sanitize_filename(username)
        safe_type = self._sanitize_filename(prompt_type)
        safe_name = self._sanitize_filename(prompt_name)

        prompt_dir = self.base_backup_dir / safe_username / safe_type / safe_name
        return prompt_dir

    def _get_next_version(self, prompt_dir: Path) -> int:
        """Get the next version number for a prompt"""
        if not prompt_dir.exists():
            return 1

        # Find all version files
        version_files = list(prompt_dir.glob("version*.json"))

        if not version_files:
            return 1

        # Extract version numbers
        versions = []
        for file in version_files:
            try:
                # Extract number from "version2.json" -> 2
                version_str = file.stem.replace("version", "")
                if version_str.isdigit():
                    versions.append(int(version_str))
                elif version_str == "":
                    # "version.json" is version 1
                    versions.append(1)
            except (ValueError, AttributeError):
                continue

        return max(versions) + 1 if versions else 1

    def backup_prompt(
        self,
        username: str,
        prompt_type: str,
        prompt_name: str,
        prompt_content: str,
        prompt_id: Optional[int] = None,
        account_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Backup a prompt to file system with automatic versioning

        Args:
            username: Account username
            prompt_type: Type of prompt
            prompt_name: Name of prompt
            prompt_content: The actual prompt content
            prompt_id: Database prompt ID (optional)
            account_id: Database account ID (optional)
            metadata: Additional metadata to store (optional)

        Returns:
            Dict with backup status and file path
        """
        try:
            # Get directory for this prompt
            prompt_dir = self._get_prompt_directory(username, prompt_type, prompt_name)
            prompt_dir.mkdir(parents=True, exist_ok=True)

            # Get next version number
            version_num = self._get_next_version(prompt_dir)

            # Create filename
            if version_num == 1:
                filename = "version.json"
            else:
                filename = f"version{version_num}.json"

            file_path = prompt_dir / filename

            # Prepare backup data
            backup_data = {
                'username': username,
                'prompt_type': prompt_type,
                'prompt_name': prompt_name,
                'prompt_content': prompt_content,
                'version': version_num,
                'backed_up_at': datetime.now().isoformat(),
                'prompt_id': prompt_id,
                'account_id': account_id,
                'metadata': metadata or {}
            }

            # Write to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)

            logger.info(
                f"Backed up prompt: {username}/{prompt_type}/{prompt_name} "
                f"as version {version_num} to {file_path}"
            )

            return {
                'success': True,
                'file_path': str(file_path),
                'version': version_num,
                'directory': str(prompt_dir)
            }

        except Exception as e:
            error_msg = f"Failed to backup prompt: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg
            }

    def get_latest_version(
        self,
        username: str,
        prompt_type: str,
        prompt_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get the latest version of a backed-up prompt"""
        try:
            prompt_dir = self._get_prompt_directory(username, prompt_type, prompt_name)

            if not prompt_dir.exists():
                return None

            # Find all version files
            version_files = list(prompt_dir.glob("version*.json"))

            if not version_files:
                return None

            # Find the highest version
            latest_file = None
            latest_version = 0

            for file in version_files:
                try:
                    version_str = file.stem.replace("version", "")
                    version_num = 1 if version_str == "" else int(version_str)

                    if version_num > latest_version:
                        latest_version = version_num
                        latest_file = file
                except (ValueError, AttributeError):
                    continue

            if latest_file:
                with open(latest_file, 'r', encoding='utf-8') as f:
                    return json.load(f)

            return None

        except Exception as e:
            logger.error(f"Failed to get latest version: {e}")
            return None

    def get_all_versions(
        self,
        username: str,
        prompt_type: str,
        prompt_name: str
    ) -> list:
        """Get all versions of a backed-up prompt"""
        try:
            prompt_dir = self._get_prompt_directory(username, prompt_type, prompt_name)

            if not prompt_dir.exists():
                return []

            versions = []
            version_files = list(prompt_dir.glob("version*.json"))

            for file in version_files:
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        versions.append(data)
                except Exception as e:
                    logger.warning(f"Could not read {file}: {e}")
                    continue

            # Sort by version number
            versions.sort(key=lambda x: x.get('version', 0))

            return versions

        except Exception as e:
            logger.error(f"Failed to get all versions: {e}")
            return []

    def restore_from_backup(
        self,
        username: str,
        prompt_type: str,
        prompt_name: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Restore a prompt from backup

        Args:
            username: Account username
            prompt_type: Type of prompt
            prompt_name: Name of prompt
            version: Specific version to restore (None = latest)

        Returns:
            Backup data if found, None otherwise
        """
        try:
            if version is None:
                return self.get_latest_version(username, prompt_type, prompt_name)

            prompt_dir = self._get_prompt_directory(username, prompt_type, prompt_name)

            if version == 1:
                file_path = prompt_dir / "version.json"
            else:
                file_path = prompt_dir / f"version{version}.json"

            if not file_path.exists():
                logger.warning(f"Backup file not found: {file_path}")
                return None

            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Failed to restore from backup: {e}")
            return None

    def list_all_backups(self) -> list:
        """List all backed-up prompts"""
        backups = []

        try:
            # Walk through directory structure
            for username_dir in self.base_backup_dir.iterdir():
                if not username_dir.is_dir():
                    continue

                username = username_dir.name

                for type_dir in username_dir.iterdir():
                    if not type_dir.is_dir():
                        continue

                    prompt_type = type_dir.name

                    for name_dir in type_dir.iterdir():
                        if not name_dir.is_dir():
                            continue

                        prompt_name = name_dir.name

                        # Count versions
                        version_files = list(name_dir.glob("version*.json"))
                        version_count = len(version_files)

                        backups.append({
                            'username': username,
                            'prompt_type': prompt_type,
                            'prompt_name': prompt_name,
                            'version_count': version_count,
                            'directory': str(name_dir)
                        })

            return backups

        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return []
