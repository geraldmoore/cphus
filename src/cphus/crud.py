from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl


class ListingsManager:
    """CRUD operations for managing listings in a Polars DataFrame.

    This class provides methods to create, read, update, and delete listings
    with automatic timestamp tracking and CQL-style filtering support.
    """

    CREATED_AT_COLUMN = "created_at"
    UPDATED_AT_COLUMN = "updated_at"

    def __init__(
        self,
        storage_path: Path | None = None,
        unique_key: str = "listing_url",
    ):
        """Initialise the CRUD manager.

        Args:
            storage_path (Path | None): Path to save/load the DataFrame. If None, data is only in memory.
            unique_key (str): Column name to use as unique identifier for listings.
        """
        self.storage_path = storage_path
        self.unique_key = unique_key
        self.df: pl.DataFrame = pl.DataFrame()
        self._load()

    def _load(self) -> None:
        """Load DataFrame from storage if it exists."""
        if self.storage_path and self.storage_path.exists():
            self.df = pl.read_parquet(self.storage_path)

    def _save(self) -> None:
        """Save DataFrame to storage if storage_path is set."""
        if self.storage_path and len(self.df) > 0:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            if self.storage_path.suffix == ".parquet":
                self.df.write_parquet(self.storage_path)
            else:
                self.df.write_csv(self.storage_path)

    def _build_filter(self, filters: dict[str, Any]) -> pl.Expr:
        """Build a Polars expression from CQL-style filters.

        Args:
            filters (dict[str, Any]): Dictionary with column names as keys and values/operators as values.
                Simple form: {"column": value}
                CQL form: {"column": {"operator": value}}

        Returns:
            pl.Expr: Polars expression combining all filters with AND logic.
        """
        conditions = []

        for column, criterion in filters.items():
            if column not in self.df.columns:
                continue

            col = pl.col(column)

            # Simple equality check
            if not isinstance(criterion, dict):
                conditions.append(col == criterion)
                continue

            # CQL-style operators
            for operator, value in criterion.items():
                if operator == "eq":
                    conditions.append(col == value)
                elif operator == "ne":
                    conditions.append(col != value)
                elif operator == "gt":
                    conditions.append(col > value)
                elif operator == "gte":
                    conditions.append(col >= value)
                elif operator == "lt":
                    conditions.append(col < value)
                elif operator == "lte":
                    conditions.append(col <= value)
                elif operator == "in":
                    conditions.append(col.is_in(value))
                elif operator == "nin":
                    conditions.append(~col.is_in(value))
                elif operator == "contains":
                    conditions.append(col.str.contains(value))
                elif operator == "startswith":
                    conditions.append(col.str.starts_with(value))
                elif operator == "endswith":
                    conditions.append(col.str.ends_with(value))
                elif operator == "is_null":
                    conditions.append(col.is_null() if value else col.is_not_null())
                elif operator == "is_not_null":
                    conditions.append(col.is_not_null() if value else col.is_null())

        # Combine all conditions with AND
        if not conditions:
            return pl.lit(True)

        result = conditions[0]
        for condition in conditions[1:]:
            result = result & condition

        return result

    def _ensure_unique(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[Any]]:
        """Filter out records that already exist.

        Args:
            records (list[dict[str, Any]]): List of record dictionaries.

        Returns:
            tuple[list[dict[str, Any]], list[Any]]: Tuple of (new_records, existing_keys) where
                existing_keys are the ones that were filtered out.
        """
        if len(self.df) == 0 or not records:
            return records, []

        existing_keys = set(self.df[self.unique_key].to_list())
        new_records = []
        duplicate_keys = []

        for record in records:
            key = record.get(self.unique_key)
            if key and key in existing_keys:
                duplicate_keys.append(key)
            else:
                new_records.append(record)

        return new_records, duplicate_keys

    def create(self, record: dict[str, Any]) -> pl.DataFrame:
        """Create a new record with automatic timestamps.

        Args:
            record (dict[str, Any]): Dictionary containing the record data.

        Returns:
            pl.DataFrame: The created record as a single-row DataFrame.

        Raises:
            ValueError: If a record with the same unique_key already exists.
        """
        key = record.get(self.unique_key)
        if key and len(self.df) > 0 and key in self.df[self.unique_key].to_list():
            raise ValueError(f"Record with {self.unique_key}={key} already exists")

        # Add timestamps
        now = datetime.now()
        record[self.CREATED_AT_COLUMN] = now
        record[self.UPDATED_AT_COLUMN] = now

        new_record_df = pl.DataFrame([record])
        self.df = pl.concat([self.df, new_record_df], how="diagonal_relaxed")
        self._save()

        return new_record_df

    def create_many(
        self, records: list[dict[str, Any]], skip_existing: bool = False
    ) -> pl.DataFrame:
        """Create multiple records at once.

        Args:
            records (list[dict[str, Any]]): List of dictionaries containing record data.
            skip_existing (bool): If True, skip records that already exist instead of raising an error.

        Returns:
            pl.DataFrame: DataFrame containing all created records.

        Raises:
            ValueError: If skip_existing is False and duplicate records are found.
        """
        if not records:
            return pl.DataFrame()

        new_records, duplicate_keys = self._ensure_unique(records)

        if duplicate_keys and not skip_existing:
            raise ValueError(
                f"Found {len(duplicate_keys)} duplicate records with keys: {duplicate_keys[:5]}"
            )

        if not new_records:
            return pl.DataFrame()

        # Add timestamps
        now = datetime.now()
        for record in new_records:
            record[self.CREATED_AT_COLUMN] = now
            record[self.UPDATED_AT_COLUMN] = now

        new_records_df = pl.DataFrame(new_records)
        self.df = pl.concat([self.df, new_records_df], how="diagonal_relaxed")
        self._save()

        return new_records_df

    def read(
        self,
        filters: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> pl.DataFrame:
        """Read records matching CQL-style filters.

        Args:
            filters (dict[str, Any] | None): CQL-style filter dictionary. Supported operators:
                - eq: equal (also default when value is not a dict)
                - ne: not equal
                - gt: greater than
                - gte: greater than or equal
                - lt: less than
                - lte: less than or equal
                - in: value in list
                - nin: value not in list
                - contains: string contains substring
                - startswith: string starts with
                - endswith: string ends with
                - is_null: value is null
                - is_not_null: value is not null
            columns (list[str] | None): List of column names to return. If None, returns all columns.
            limit (int | None): Maximum number of records to return.
            offset (int | None): Number of records to skip.

        Returns:
            pl.DataFrame: DataFrame containing matching records.
        """
        if len(self.df) == 0:
            return pl.DataFrame()

        result = self.df

        # Apply filters
        if filters:
            filter_expr = self._build_filter(filters)
            result = result.filter(filter_expr)

        # Apply offset and limit
        if offset:
            result = result.slice(offset, result.height)
        if limit:
            result = result.limit(limit)

        # Select columns
        if columns:
            available_columns = [col for col in columns if col in result.columns]
            if available_columns:
                result = result.select(available_columns)

        return result

    def read_all(self) -> pl.DataFrame:
        """Read all records.

        Returns:
            pl.DataFrame: DataFrame containing all records.
        """
        return self.df.clone()

    def update(
        self,
        key_value: Any,
        updates: dict[str, Any],
    ) -> pl.DataFrame:
        """Update a record by its unique key.

        Args:
            key_value (Any): Value of the unique_key to identify the record.
            updates (dict[str, Any]): Dictionary of column: value pairs to update.

        Returns:
            pl.DataFrame: DataFrame containing the updated record.

        Raises:
            ValueError: If the record is not found.
        """
        if len(self.df) == 0:
            raise ValueError("No records to update")

        # Check if record exists
        mask = pl.col(self.unique_key) == key_value
        if not self.df.filter(mask).height:
            raise ValueError(f"Record with {self.unique_key}={key_value} not found")

        # Add update timestamp
        updates[self.UPDATED_AT_COLUMN] = datetime.now()

        # Build update expression for each column
        update_exprs = []
        for column, value in updates.items():
            update_exprs.append(
                pl.when(mask).then(pl.lit(value)).otherwise(pl.col(column)).alias(column)
            )

        # Add unchanged columns
        unchanged_cols = [col for col in self.df.columns if col not in updates]
        for col in unchanged_cols:
            update_exprs.append(pl.col(col))

        self.df = self.df.select(update_exprs)
        self._save()

        return self.df.filter(mask)

    def update_many(
        self,
        filters: dict[str, Any],
        updates: dict[str, Any],
    ) -> pl.DataFrame:
        """Update multiple records matching CQL-style filters.

        Args:
            filters (dict[str, Any]): CQL-style filter dictionary to identify records to update.
            updates (dict[str, Any]): Dictionary of column: value pairs to update.

        Returns:
            pl.DataFrame: DataFrame containing all updated records.
        """
        if len(self.df) == 0:
            return pl.DataFrame()

        filter_expr = self._build_filter(filters)
        updates[self.UPDATED_AT_COLUMN] = datetime.now()

        # Build update expressions
        update_exprs = []
        for column, value in updates.items():
            update_exprs.append(
                pl.when(filter_expr).then(pl.lit(value)).otherwise(pl.col(column)).alias(column)
            )

        unchanged_cols = [col for col in self.df.columns if col not in updates]
        for col in unchanged_cols:
            update_exprs.append(pl.col(col))

        self.df = self.df.select(update_exprs)
        self._save()

        return self.df.filter(filter_expr)

    def delete(self, key_value: Any) -> bool:
        """Delete a record by its unique key.

        Args:
            key_value (Any): Value of the unique_key to identify the record.

        Returns:
            bool: True if the record was deleted, False if it wasn't found.
        """
        if len(self.df) == 0:
            return False

        initial_len = len(self.df)
        self.df = self.df.filter(pl.col(self.unique_key) != key_value)
        deleted = len(self.df) < initial_len

        if deleted:
            self._save()

        return deleted

    def delete_many(
        self, key_values: list[Any] | None = None, filters: dict[str, Any] | None = None
    ) -> int:
        """Delete multiple records by keys or CQL-style filters.

        Args:
            key_values (list[Any] | None): List of unique_key values to delete.
            filters (dict[str, Any] | None): CQL-style filter dictionary to identify records to delete.

        Returns:
            int: Number of records deleted.
        """
        if len(self.df) == 0:
            return 0

        initial_len = len(self.df)

        if key_values:
            self.df = self.df.filter(~pl.col(self.unique_key).is_in(key_values))
        elif filters:
            filter_expr = self._build_filter(filters)
            self.df = self.df.filter(~filter_expr)
        else:
            return 0

        deleted = initial_len - len(self.df)

        if deleted > 0:
            self._save()

        return deleted

    def find_new_listings(
        self,
        new_listings: list[dict[str, Any]],
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Find new listings that don't exist in the current DataFrame.

        Args:
            new_listings (list[dict[str, Any]]): List of dictionaries containing listing data.

        Returns:
            tuple[pl.DataFrame, pl.DataFrame]: Tuple of (new_listings_df, existing_listings_df).
        """
        if len(self.df) == 0:
            return pl.DataFrame(new_listings), pl.DataFrame()

        new_listings_df = pl.DataFrame(new_listings)
        existing_keys = set(self.df[self.unique_key].to_list())

        new_mask = ~pl.col(self.unique_key).is_in(list(existing_keys))
        new_listings_filtered = new_listings_df.filter(new_mask)
        existing_listings = new_listings_df.filter(~new_mask)

        return new_listings_filtered, existing_listings

    def add_new_listings(
        self,
        new_listings: list[dict[str, Any]],
    ) -> tuple[int, pl.DataFrame]:
        """Find and add new listings that don't exist yet.

        Args:
            new_listings (list[dict[str, Any]]): List of dictionaries containing listing data.

        Returns:
            tuple[int, pl.DataFrame]: Tuple of (count, new_listings_df).
        """
        new_listings_df, _ = self.find_new_listings(new_listings)

        if len(new_listings_df) == 0:
            return 0, pl.DataFrame()

        created_df = self.create_many(new_listings_df.to_dicts(), skip_existing=True)
        return len(created_df), created_df

    def count(self, filters: dict[str, Any] | None = None) -> int:
        """Count records matching CQL-style filters.

        Args:
            filters (dict[str, Any] | None): Optional CQL-style filter dictionary.

        Returns:
            int: Number of matching records.
        """
        if filters:
            return len(self.read(filters))
        return len(self.df)

    def exists(self, key_value: Any) -> bool:
        """Check if a record exists by its unique key.

        Args:
            key_value (Any): Value of the unique_key to check.

        Returns:
            bool: True if the record exists, False otherwise.
        """
        if len(self.df) == 0:
            return False
        return key_value in self.df[self.unique_key].to_list()
