# Table Count Utility

This utility calculates the total number of records in a specified MySQL table. Considering the performance with large tables, it confirms that the `id` column is a numeric primary key and then calculates the total count by dividing the range into steps and summing the counts from each range.

## Prerequisites

- Go language installed.
- Access to a MySQL database.

## About Performance

Instead of directly executing `SELECT COUNT(id) FROM table_name` on tables with large amounts of data, this tool optimizes performance by employing the following strategy:

- It confirms that the table's `id` column is a primary key and is numeric.
- The tool uses a user-specified step size (or the default of 100000) to divide the ID range into segments.
- For each ID range, it executes a query in the format `SELECT COUNT(id) FROM table_name WHERE id BETWEEN x AND y` to get the partial counts.
- It sums up the counts from each range to determine the total number of records in the table.

This approach allows for efficient calculation of total records even in large tables.

## Usage

1. First, set the `DATABASE_URL` environment variable. The format is as follows:

   ```
   export DATABASE_URL="username:password@tcp(hostname:port)/dbname"
   ```

   Replace `username`, `password`, `hostname`, `port`, and `dbname` with the appropriate values.

2. (Optional) Set the `STEP_SIZE` environment variable to specify the step size for counting. The default value is `100000`.

   ```
   export STEP_SIZE=5000
   ```

3. Execute the program to count the total records in the table:

   ```
   go run main.go <table_name>
   ```

   Replace `<table_name>` with the name of the table you want to count.

## Notes

- This utility works only with tables where the `id` column is a numeric primary key.
- Be cautious when using this on tables with a large amount of data or in high-load environments. Adjust `STEP_SIZE` as necessary.
