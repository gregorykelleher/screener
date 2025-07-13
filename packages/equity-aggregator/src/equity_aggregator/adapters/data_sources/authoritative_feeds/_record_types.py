# authoritative_feeds/_record_types.py

from collections.abc import AsyncIterator, Callable

# a single equity record
EquityRecord = dict[str, object]

# an async stream of records
RecordStream = AsyncIterator[EquityRecord]

# a function that extracts a unique key from an EquityRecord
RecordUniqueKeyExtractor = Callable[[EquityRecord], object]

# a function that takes a RecordStream and returns a deduplicated RecordStream
UniqueRecordStream = Callable[[RecordStream], RecordStream]
