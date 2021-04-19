# THE REPORT2BQ POSTPROCESSOR

* Author: David Harcombe (davidharcombe@google.com)
* Type: Open source
* Status: Production

## OVERVIEW

The `postprocessor` allows you to hook a Python function to the end of a
successfully completed csv import (the successful completion of the
`report2bq-loader` function). This code must meet certain criteria (see below),
and is stored in a _specific_ Cloud Storage bucket. This bucket **CANNOT** be
changed.

The code is dynamically loaded on demand from the bucket, and no cloud functions
have to be re-deployed in order to add, update or delete a `postprocessor`.

`Postprocessor`s have full access to the system, so should be treated with care,
but they can be a powerful addition to your Report2BQ installation.

## PREREQUISITES

* A fully working install of Report2BQ.
* The `[project]-report2bq-postprocessor` bucket must be present. This is part
of the standard installation.

## CREATING A `POSTPROCESSOR`

**NOTE**: Example `postprocessor`s can be found in the `postprocessors`
folder.

1. Create a new python file.

3. Import the `postprocessor` classes.
  * `from classes.postprocessor import PostProcessor`
    This class contains the base methods needed for a post processor along with
    two helper methods for use with Big Query.
  * The Big Query helpers are:
    * `execute_and_wait(query: str) -> google.cloud.bigquery.table.RowIterator`
      Runs the query defined in the `query` variable and returns the iterator
      containing the result
    * `check_table_exists(project: str, dataset: str, table: str) -> bool`
      Checks to see if a table is present in the given project/dataset.

2. Create the `Processor` class, extending the parent.
   * `class Processor(PostProcessor):`
   * The class **MUST** be named `Processor` as this is the **ONLY** class that
     will be loaded at runtime.

3. Implement the `run(context=None, **attributes: Mapping[str, str]) -> Dict[str, Any])` method.
   * A set of attributes are passed on each invocation. They are:
     * `id`: the job id, which is the id of the report that was the source of
       the data.
     * `project`: the destination project for the finished import.
     * `dataset`: the destination dataset for the finished import.
     * `table`: the destination table name for the finished import.
     * `rows`: the total number of rows imported.
     * `type`: the GMP product that was the source of the data. This will be one
       of `dv360`, `cm`, `adh`, `sa360`, `sa360_report` or `ga360_report`
     * `columns`: a semi-colon (`;`) delimited list of the column names in the
       destination table.

1. Create any other postprocessor actions. These can be called from the `run()`
   method and can even contain other classes, dataclasses etc - but EVERYTHING
   **MUST** be defined in this one Python file. Libraries available to you
   include any standard Python 3.8 libraries and anything listed in the
   `requirements.txt` file. Should you require any other libraries, you will
   need to alter the `requirements.txt` file and redeploy the `postprocessor`
   with the command:
   ```
   ./install.sh --project [project] --administrator [admin email] --deploy-postprocessor
   ```
   **NOTE**: Should you do this, make sure you check dependencies etc very
   carefully.

### Things to remember when creating a `Postprocessor`

1. When running, your maximum memory footprint for any processing is 4Gb, which
   is the maximum available to any cloud function in a Google Cloud Project.
1. You have a maximum runtime for any process of 540s (9 minutes). Again, this
   is a system cap on all cloud functions.
1. If you're curious as to where memory and execution time are going in your
   `postprocessor` and you're looking for optimizations... check out the
   `classes/decorators.py` file and look at the two Python decorators called
   `@timeit` (for just timing a function) and `@measure_memory` (for timing a
   function and seeing how much memory it uses). If you do use them, you will
   need access to the GCP Stackdriver logs as that is where the output goes.

## TESTING YOUR `POSTPROCESSOR`

1. Deploy the `postprocessor` file (see below). It **MUST** be deployed in the correct
   production bucket. `Postprocessor`s will only ever be loaded from there, even
   in test.
1. Run the `postprocessor.py` file in the cli folder
   * Take careful note of the parameters listed.
   * You will **NEED** to pass in the values that the system would normally
     populate for you from the successful job import. These are:
     * `name` - this is the name of your `postprocessor` file, without the '.py'
     * `project` - the project id
     * `dataset` - the dataset containing the imported table, defaults to `report2bq`
     * `table` - the imported table
     * `report_id` - the report that created the import file
     * `product` - one of dv360, cm, adh, ga360, sa360, sa360_report
     * `rows` - number of rows imported
     * `columns` - names of the columns in the created table, '`;`' separated
   * `name`, `dataset`, and `table` are pretty much essential. The others really
    are up to you, and will depend upon what your `postprocessor` needs. They are,
    however, all passed from the import job and as such are always available to your
    `postprocessor`.
   * Your `postprocessor` also has access to Firestore, so can access the configuration
     of the job if it knows the `product` and `report_id`. This however is beyond
     the scope of a simple doc like this; you should look into the `report2bq` code
     itself for further details.

## DEPLOYING A `POSTPROCESSOR`

Copy the python file to the `[project]-report2bq-postprocessor` bucket.

## CALLING THE DEPLOYED `POSTPROCESSOR`

Create your `fetcher` job with the `--message` parameter. The value of
`--message` should be the name of the python file, without the extension. Hence
if you have created `my_post_processor.py` and copied it into the bucket, you
would then create your `fetcher` using the commend (for a DV360 `fetcher`):
```
./create_fetcher.sh --project [project] --email [report owner email] --report-id [id] \
 --message my_post_processor
```

If you're upgrading an existing job, that's fine; report2bq will simply delete
the old job and create a new one with a new execution minute. To keep the old
execution time, simply use:
```
./create_fetcher.sh --project [project] --email [report owner email] --report-id [id] \
 --message my_post_processor --timer [minute value]
```
