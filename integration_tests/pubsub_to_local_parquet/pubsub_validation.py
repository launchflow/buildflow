import os
import time

import pyarrow.parquet as pq

_TIMEOUT = 60

expected_data = {"output_val": 2}
file_path = os.environ["OUTPUT_FILE_PATH"]

match = False

start_time = time.time()

while time.time() - start_time < _TIMEOUT:
    try:
        table = pq.read_table(file_path)
    except FileNotFoundError:
        print("File not found, waiting 2 seconds and trying again.")
        time.sleep(2)
        continue
    assert [expected_data] == table.to_pylist(), (
        f"Failed to match output data: got: {table.to_pylist()} want: "
        f"{expected_data}"
    )
    match = True
    break

if not match:
    raise AssertionError(f"Failed to find match in file: {file_path}")
print(f"Found match in file: {file_path}")
