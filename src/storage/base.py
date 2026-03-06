"""Base storage abstraction interfaces for extensibility."""

from abc import ABC, abstractmethod
from typing import IO, List, Optional


class FileMeta:
    """Represents metadata of a file in storage."""

    def __init__(self, path: str, size: int, modified_time: float):
        self.path = path
        self.size = size
        self.modified_time = modified_time

    def __repr__(self) -> str:
        return f"FileMeta(path={self.path}, size={self.size}, modified_time={self.modified_time})"


class BaseStorageReader(ABC):
    """Abstract base class for reading from storage systems."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the storage system."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the storage system."""
        pass

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[FileMeta]:
        """List all files under the given prefix/directory.

        Args:
            prefix: The directory or prefix to list files from.

        Returns:
            A list of FileMeta objects.
        """
        pass

    @abstractmethod
    def read_stream(self, path: str) -> IO[bytes]:
        """Get a read stream for the file at the specified path.

        Args:
            path: The path to the file to read.

        Returns:
            A file-like object supporting read operations.
        """
        pass

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """Delete the file at the specified path.

        Args:
            path: The path to the file to delete.
        """
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class BaseStorageWriter(ABC):
    """Abstract base class for writing to storage systems."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the storage system."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the storage system."""
        pass

    @abstractmethod
    def write_stream(self, path: str, stream: IO[bytes]) -> None:
        """Write data from a stream to the specified path.

        Args:
            path: The destination path.
            stream: A file-like object supporting read operations.
        """
        pass

    @abstractmethod
    def get_meta(self, path: str) -> Optional[FileMeta]:
        """Get metadata for a file if it exists, else None.

        Args:
            path: The path to the file.

        Returns:
            FileMeta if file exists, else None.
        """
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class BaseTransformer(ABC):
    """Abstract base class for streaming data transformations."""

    @abstractmethod
    def transform(self, stream: IO[bytes]) -> IO[bytes]:
        """Transform an input stream into an output stream.

        Args:
            stream: The input stream.

        Returns:
            The transformed stream.
        """
        pass
