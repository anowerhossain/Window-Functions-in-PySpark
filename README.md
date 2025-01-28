## PySpark Window Functions 🎯

This repository demonstrates how to use various window functions in PySpark with a sample dataset. The script performs advanced transformations like row numbering, ranking, and accessing lead/lag values using the Spark DataFrame API.

- Install `pyspark` using pip:
```bash
pip install pyspark
```

### Dataset
The script uses a small, hardcoded dataset representing sales data:

| salesperson_id | region | sales_amount |
|----------------|--------|--------------|
| 1              | North  | 500          |
| 2              | North  | 500          |
| 3              | North  | 400          |
| 4              | South  | 700          |
| 5              | South  | 700          |
| 6              | South  | 600          |


### 1. **Row Number**
Assigns a unique sequential number to each row in a partition, ordered by `sales_amount`.

### 2. **Rank**
Assigns a rank to each row in a partition, with gaps in ranking for duplicate values.

### 3. **Dense Rank**
Assigns a rank to each row in a partition, without gaps in ranking for duplicate values.

### 4. **Lead**
Accesses the value of the next row within the same partition, relative to the current row.

### 5. **Lag**
Accesses the value of the previous row within the same partition, relative to the current row.

### 6. **First Value**
Returns the first value in the window partition.

### 7. **Last Value**
Returns the last value in the window partition. The script ensures it retrieves the true last value of the partition by explicitly defining the window frame.

### Example Output

The resulting DataFrame will look like this:

| salesperson_id | region | sales_amount | row_number | rank | dense_rank | lead_value | lag_value | first_value | last_value |
|----------------|--------|--------------|------------|------|------------|------------|-----------|-------------|------------|
| 3              | North  | 400          | 1          | 1    | 1          | 500        | null      | 400         | 500        |
| 1              | North  | 500          | 2          | 2    | 2          | 500        | 400       | 400         | 500        |
| 2              | North  | 500          | 3          | 2    | 2          | null       | 500       | 400         | 500        |
| 6              | South  | 600          | 1          | 1    | 1          | 700        | null      | 600         | 700        |
| 4              | South  | 700          | 2          | 2    | 2          | 700        | 600       | 600         | 700        |
| 5              | South  | 700          | 3          | 2    | 2          | null       | 700       | 600         | 700        |

### Notes 🔑
- The script defines a custom window specification for `last_value` using:
  ```python
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  ```
  This ensures the function retrieves the true last value of the partition.

- Modify the dataset or add additional transformations as needed for your use case.
