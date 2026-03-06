"""SFTP storage implementation for reading and writing files."""

import os
import stat
import shutil
import logging
from typing import IO, List, Optional

import paramiko

from src.storage.base import BaseStorageReader, BaseStorageWriter, FileMeta

logger = logging.getLogger(__name__)


class SFTPConnectionBase:
    """Base class for handling SFTP connections."""

    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None

    def connect(self) -> None:
        """Establish the SSH and SFTP connection."""
        if self.sftp_client is not None:
            return

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        logger.info(
            f"Connecting to SFTP server {self.host}:{self.port} as {self.username}"
        )
        self.ssh_client.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            look_for_keys=False,
            allow_agent=False,
        )
        self.sftp_client = self.ssh_client.open_sftp()
        logger.info(f"Successfully connected to {self.host}")

    def disconnect(self) -> None:
        """Close the SFTP and SSH connections."""
        if self.sftp_client:
            self.sftp_client.close()
            self.sftp_client = None
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None
        logger.info(f"Disconnected from {self.host}")

    def _ensure_connected(self):
        if not self.sftp_client:
            self.connect()


class SFTPReader(SFTPConnectionBase, BaseStorageReader):
    """SFTP implementation of BaseStorageReader."""

    def list_files(self, prefix: str = "/") -> List[FileMeta]:
        """List all files under the given directory recursively."""
        self._ensure_connected()
        files_meta = []

        def _walk(directory: str):
            try:
                for entry in self.sftp_client.listdir_attr(directory):
                    path = os.path.join(directory, entry.filename).replace("\\", "/")
                    if stat.S_ISDIR(entry.st_mode):
                        _walk(path)
                    elif stat.S_ISREG(entry.st_mode):
                        # Ensure we always deal with relative paths without leading slash for easier matching
                        rel_path = path.lstrip("/")
                        files_meta.append(
                            FileMeta(
                                path=rel_path,
                                size=entry.st_size,
                                modified_time=entry.st_mtime,
                            )
                        )
            except IOError as e:
                logger.error(f"Failed to list directory {directory}: {e}")

        _walk(prefix)
        return files_meta

    def read_stream(self, path: str) -> IO[bytes]:
        """Get a read stream for the file. SFTPFile is a file-like object."""
        self._ensure_connected()
        absolute_path = "/" + path.lstrip("/")
        logger.debug(f"Opening read stream for {absolute_path}")
        return self.sftp_client.open(absolute_path, "rb")

    def delete_file(self, path: str) -> None:
        """Delete the file from the SFTP server."""
        self._ensure_connected()
        absolute_path = "/" + path.lstrip("/")
        logger.info(f"Deleting file {absolute_path} from source")
        self.sftp_client.remove(absolute_path)


class SFTPWriter(SFTPConnectionBase, BaseStorageWriter):
    """SFTP implementation of BaseStorageWriter."""

    def _mkdir_p(self, remote_directory: str) -> None:
        """Emulate mkdir -p on the SFTP server."""
        self._ensure_connected()
        if remote_directory == "/":
            return

        try:
            self.sftp_client.stat(remote_directory)
        except IOError:
            parent = os.path.dirname(remote_directory)
            if parent and parent != "/":
                self._mkdir_p(parent)
            logger.debug(f"Creating directory {remote_directory}")
            self.sftp_client.mkdir(remote_directory)

    def write_stream(self, path: str, stream: IO[bytes]) -> None:
        """Write data from stream to the destination path using chunks."""
        self._ensure_connected()
        absolute_path = "/" + path.lstrip("/")

        # Ensure parent directory exists
        parent_dir = os.path.dirname(absolute_path)
        self._mkdir_p(parent_dir)

        logger.info(f"Writing stream to {absolute_path}")

        # Use shutil.copyfileobj to stream data. Default chunk size is usually 64KB or 1MB.
        # This keeps memory utilization flat regardless of file size.
        with self.sftp_client.open(absolute_path, "wb") as dest_stream:
            # 1MB chunk size
            shutil.copyfileobj(stream, dest_stream, length=1024 * 1024)

        logger.info(f"Finished writing to {absolute_path}")

    def get_meta(self, path: str) -> Optional[FileMeta]:
        """Check if file exists and return its metadata."""
        self._ensure_connected()
        absolute_path = "/" + path.lstrip("/")
        try:
            file_stat = self.sftp_client.stat(absolute_path)
            if stat.S_ISREG(file_stat.st_mode):
                return FileMeta(
                    path=path.lstrip("/"),
                    size=file_stat.st_size,
                    modified_time=file_stat.st_mtime,
                )
            return None
        except IOError:
            # File does not exist
            return None
