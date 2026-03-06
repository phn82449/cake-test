"""Operator for transferring files between extensible storage systems."""

import logging
from typing import Any, List, Optional

from airflow.sdk.bases.hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from src.storage.base import BaseStorageReader, BaseStorageWriter, BaseTransformer
from src.storage.sftp import SFTPReader, SFTPWriter

logger = logging.getLogger(__name__)


class StorageFactory:
    """Factory to create storage readers and writers based on Airflow connection type."""

    @staticmethod
    def get_reader(conn_id: str) -> BaseStorageReader:
        conn = BaseHook.get_connection(conn_id)
        if conn.conn_type == "sftp":
            return SFTPReader(
                host=conn.host,
                port=conn.port or 22,
                username=conn.login,
                password=conn.password,
            )
        raise ValueError(f"Unsupported connection type for reader: {conn.conn_type}")

    @staticmethod
    def get_writer(conn_id: str) -> BaseStorageWriter:
        conn = BaseHook.get_connection(conn_id)
        if conn.conn_type == "sftp":
            return SFTPWriter(
                host=conn.host,
                port=conn.port or 22,
                username=conn.login,
                password=conn.password,
            )
        raise ValueError(f"Unsupported connection type for writer: {conn.conn_type}")


class ExtensibleTransferOperator(BaseOperator):
    """
    Transfers files from a source storage to a target storage,
    preserving directory structure and handling only new/modified files.
    """

    def __init__(
        self,
        *,
        source_conn_id: str,
        target_conn_id: str,
        source_prefix: str = "/",
        target_prefix: str = "/",
        transformers: Optional[List[BaseTransformer]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.source_prefix = source_prefix
        self.target_prefix = target_prefix
        self.transformers = transformers or []

    def execute(self, context: Context) -> Any:
        logger.info(
            f"Starting transfer from {self.source_conn_id} to {self.target_conn_id}"
        )

        with (
            StorageFactory.get_reader(self.source_conn_id) as reader,
            StorageFactory.get_writer(self.target_conn_id) as writer,
        ):
            # 1. List files in source
            logger.info(f"Listing files in source prefix: {self.source_prefix}")
            source_files = reader.list_files(self.source_prefix)
            logger.info(f"Found {len(source_files)} files in source.")

            synced_count = 0
            skipped_count = 0

            # 2. Iterate and sync
            for source_file in source_files:
                # Calculate target path (preserving relative structure)
                # Ensure we strip leading slash from source_prefix for relative path calculation
                clean_prefix = self.source_prefix.lstrip("/")

                if source_file.path.startswith(clean_prefix):
                    rel_path = source_file.path[len(clean_prefix) :].lstrip("/")
                else:
                    rel_path = source_file.path

                target_path = f"{self.target_prefix.rstrip('/')}/{rel_path}".lstrip("/")

                # 3. Check if file needs to be synced (idempotency)
                target_meta = writer.get_meta(target_path)

                # Simple strategy: Sync if target doesn't exist, or if source is newer/different size
                needs_sync = False
                if not target_meta:
                    needs_sync = True
                    logger.debug(
                        f"File {source_file.path} missing in target. Will sync."
                    )
                elif (
                    source_file.size != target_meta.size
                    or source_file.modified_time > target_meta.modified_time
                ):
                    needs_sync = True
                    logger.debug(f"File {source_file.path} changed. Will sync.")

                if not needs_sync:
                    skipped_count += 1
                    continue

                # 4. Perform the transfer via streaming
                logger.info(f"Transferring {source_file.path} -> {target_path}")
                try:
                    with reader.read_stream(source_file.path) as in_stream:
                        current_stream = in_stream
                        # Apply transformers if any
                        for transformer in self.transformers:
                            current_stream = transformer.transform(current_stream)

                        writer.write_stream(target_path, current_stream)

                    # 5. Delete the file from source after successful transfer
                    logger.info(
                        f"Successfully transferred. Deleting source file: {source_file.path}"
                    )
                    reader.delete_file(source_file.path)

                    synced_count += 1
                except Exception as e:
                    logger.error(f"Error transferring {source_file.path}: {e}")
                    raise

            logger.info(
                f"Transfer complete. Synced and deleted: {synced_count}, Skipped: {skipped_count}"
            )
