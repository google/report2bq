# SA360 MANAGER OPERATION

* Author: David Harcombe (davidharcombe@google.com)
* Type: Open source
* Status: Production

## OVERVIEW

The SA360 Manager is a way of adding and validating new SA360 Dynamic reports
and runners without the need to run shell scripts. Everything is performed
using json files stored in a specific GCS bucket. All the user needs is access
to this bucket and their OAuth token registered with Report2BQ.

## PREREQUISITES

1. An up to date version of Report2BQ installed and functioning in a Google
Cloud Project.
1. Access to the `[project]-report2bq-sa360-manager` bucket in Cloud Storage.
   - `[project]` should be replaced with your project name.
   - This bucket will be created by the installer, but if it isn't, you'll
     need to create it.

## OPERATIONS

Please note, all the examples use the `gsutil` command line to handle any Cloud
Storage operations. Using the browser to drop files into the bucket is equally
efficacious, but would require more screenshots.

### List

This will list all available (defined) SA360 dynamic reports along with all
their currently defined scheduled runners.

1. Create a file (0 bytes is fine) called `report2bq.list`.
2. Copy to the `sa360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. Check the `report_list.results` file.
5. The completed action will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again.

#### Example:
```
$ touch report2bq.list
$ gsutil cp report2bq.list gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/report2bq.list.processed
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/report_list.results
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/report_list.results
SA360 Dynamic Reports defined for project report2bq-zz9-plural-z-alpha
  sa360_hourly_depleted
    sa360_hourly_depleted_20100000000000616_21700000000027342
    sa360_hourly_depleted_20100000000000616_21700000000028310
    sa360_hourly_depleted_20100000000000616_21700000000028644
    sa360_hourly_depleted_20100000000000616_21700000001449600
    sa360_hourly_depleted_20100000000000885_21700000001469687
    sa360_hourly_depleted_20500000000000136_21500000000002096
...
```

### Show

This will extract the `json` definition of the named report from Firestore and
store it as a file.

1. Create a file (0 bytes is fine) named `[report].show`.
2. Copy to the `sa360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. Check the `[report].results` file.
5. The completed action will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again.

#### Example:
```
$ touch sa360_hourly_depleted.show
$ gsutil cp sa360_hourly_depleted.show gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/sa360_hourly_depleted.results
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/sa360_hourly_depleted.show.processed
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/sa360_hourly_depleted.results
{
  "parameters": [
    {
      "name": "AgencyId",
      "element_type": "int",
      "path": "reportScope.agencyId"
    },
    {
      "name": "AdvertiserId",
      "path": "reportScope.advertiserId",
      "element_type": "int"
    }
...
    "maxRowsPerFile": 10000000,
    "includeRemovedEntities": "False",
    "statisticsCurrency": "agency"
  }
}
```

### Add

This will add a new SA360 dynamic report, based on the supplied `json` file.
The file should be in the following structure:
```
{
  "report2bq_name": *string*,
  "parameters" :[
    {
      "name": *string*,
      "path": *string*,
      "element_type": *string*
    },
    ...
  ],
  "report": {
    ...
  }
}
```

| Property name | Type | Description | Notes |
| --- | --- | --- | --- |
| **report2bq_name** | string | The name of the report ||
| **parameters[]** | list | The list of parameters | This list will be as long as the number of parameters to be replaced. At a minimum, it should contain 4 elements: **AgencyId**, **AdvertiserId**, **StartDate** and **EndDate** *exactly* as shown here. Other columns can be defined as below. |
| **parameters[].name** | string | The name of the parameter | Case sensitive. This is what the final runner should know about to pass in. |
| **parameters[].element_type** | string | The type of the parameter | Valid values are: int, string. If this element is not present, `string` is assumed as default. |
| **parameters[].path** | string | The `json` path to the parameter | The path to the element within the sa360 report. For example: `"reportScope.agencyId"` indicates that in the report defined below, the element to be replaced at execution time can be found at the `json` doc path `{ "reportScope": { "agencyId": "" } }`. If the value is `"columns"`, then this is assumed to be a column definition and `is_list` MUST be true. For more on columns, see below. |
| **parameters[].is_list** | boolean | Is this a column list value | Indicates that this points to the name of a column in the final report output. |
| **report** | object | The SA360 report definition | This is a standard `json`-format SA360 report, as defined in the [SA360 Reporting API Docs](https://developers.google.com/search-ads/v2/reference/reports/generate?hl=en). The report should be embedded here as it will be dynamically loaded and the values in the parameters defined above will be substituted in at execution time. It is suggested that for clarity, any substituted values should be left blank. (In the full example below, see `reportScope.agencyId` as an example. Report *columns* to be substituted should have a column key of `c` (or any other consistent, but irrelevant value since the object key cannot be empty) as the value of the Parameter name as the object value, eg: `{ "c": "ConversionMetric" }` |


**A full example report definition**
```
{
  "report2bq_name": "conversions_and_revenue",
  "parameters": [
    {
      "name": "AgencyId",
      "path": "reportScope.agencyId",
      "element_type": "int"
    },
    {
      "name": "AdvertiserId",
      "path": "reportScope.advertiserId",
      "element_type": "int"
    },
    {
      "name": "StartDate",
      "path": "timeRange.startDate"
    },
    {
      "name": "EndDate",
      "path": "timeRange.endDate"
    },
    {
      "name": "ConversionMetric",
      "path": "columns",
      "is_list": true
    },
    {
      "name": "RevenueMetric",
      "path": "columns",
      "is_list": true
    }
  ],
  "report": {
    "reportScope": {
      "agencyId": "",
      "advertiserId": ""
    },
    "reportType": "campaign",
    "columns": [
      {
        "columnName": "date"
      },
      {
        "columnName": "agency"
      },
      {
        "columnName": "agencyId"
      },
      {
        "columnName": "advertiser"
      },
      {
        "columnName": "advertiserId"
      },
      {
        "columnName": "accountType"
      },
      {
        "columnName": "account"
      },
      {
        "columnName": "accountEngineId"
      },
      {
        "columnName": "campaignEngineId"
      },
      {
        "columnName": "campaign"
      },
      {
        "columnName": "campaignType"
      },
      {
        "columnName": "effectiveBidStrategy"
      },
      {
        "columnName": "dailyBudget"
      },
      {
        "columnName": "impr"
      },
      {
        "columnName": "clicks"
      },
      {
        "columnName": "cost"
      },
      {
        "c": "ConversionMetric"
      },
      {
        "c": "RevenueMetric"
      }
    ],
    "timeRange": {
      "startDate": "",
      "endDate": ""
    },
    "filters": [
    ],
    "downloadFormat": "csv",
    "maxRowsPerFile": 10000000,
    "statisticsCurrency": "agency",
    "verifySingleTimeZone": "False",
    "includeRemovedEntities": "False"
  }
}
```

**Process**
1. Create a file named `[report].add` containing the defintion as described
   above
2. Copy to the `sa360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. The completed file will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again. This indicates a successful completion.


### Delete

This will delete the stored definition of the named report from Firestore and
disable (**not** delete) all the scheduled runners (aka **Jobs**) in the Cloud
scheduler that depend on it.

**Process**
1. Create a file containing the email address of the authorised scheduler user,
named `[report].delete`.
2. Copy to the `sa360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. Check the `[report].results` file.
5. The completed action will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again. This indicates a successful completion.

### Validate

This will check the list of `json` objects contained in the file against their
reports to ensure that a valid runner can be created. This will include checking
that all the columns specified in the report are correct.

The `json` report definitions follow this defintion:
```
{
    "report": *string*,
    "email": *string*,
    "dest_dataset": *string*,
    "minute": *string*,
    "timezone": *string*,
    "description": *string*
    "offset": *int*,
    "lookback": *int*,
    "notifier": {
        "topic": *string*,
        "message": *string*
    },
    "agencyName": *string*,
    "AgencyId": *string*,
    "advertiserName": *string*,
    "AdvertiserId": *string*,
    "ConversionMetric": {
        "type": "savedColumnName",
        "value": "[Conv] NM Checkout + Purchasers",
    },
    "RevenueMetric": {
        "type": "savedColumnName"
        "value": "[Revenue] NM Checkout + Purchasers",
    },
},
```

| Property name | Type | Description | Notes |
| --- | --- | --- | --- |
| **report** | string | The name of the ossociated report. | This is the job that will be executed when the scheduler executes this defintion. |
| **email** | string | OAuth email | The is the associated email token under which the report should be run. This user must have access to the SA360 reporting API. |
| **dest_dataset** | string | Destination dataset. | Which dataset should the results be loaded into. |
| **hour** | string | `cron` hour. | **OPTIONAL** At which hour should this job be executed. Default is `*` (or hourly). |
| **minute** | string | `cron` minute. | **OPTIONAL** At which minute in the hour should this job be executed. Default will be a random minute in the hour. |
| **timezone** | string | TZ String | Which timezone should the report assume for the SA360 data. |
| **description** | string | The job description | A descriptive name for the scheduled job. |
| **offset** | int | Start date offset | **OPTIONAL** Start date is defined as `(today - offset)`. If undefined, offset is 0 meaning reports contain today's data. |
| **lookback** | int | End date lookback | **OPTIONAL** Lookback days. This defines the end date, as `(today - lookback)`. If 0, the report will only run for today. |
| **notifier** | object | The postprocessor | **OPTIONAL** Please see the main **Report2BQ** documentation for a full explanation of the Postprocessor. |
| **notifier.topic** | string |  | The topic on which the postprocessor is listening, and to which the postprocessor message will be sent. |
| **notifier.message** | string | | The postprocessor to execute on a successful report completion |
| **agencyName** | string | Agency Name | The name of the agency for which this job will run |
| **AgencyId** | string | Agency ID | The agency ID |
| **advertiserName** | string | Advertiser Name | The name of the advertiser for which this job will run |
| **AdvertiserId** | string | Advertiser ID | The advertiser ID |
| ***report column*** | object | Report Columns | These should be one for each of the columns in the report definition. If a column is not opresent, it will be skipped. |
| ***report column.type*** | string | | The column type. This must be one of `columnName` or `savedColumnName`, depending upon if it is an advertiser-defined custom column or a standard SA360 available column. The validator will check these. |
| ***report column.value*** | string | | This is the name of the column to insert in the report. **IMPORTANT**: This is case and punctuation sensitive and must match the value in SA360 ***EXACTLY***.


**A full example**
```
[
  {
    "report": "conversion_and_revenue",
    "email": "*****@google.com",
    "dest_dataset": "report2bq",
    "minute": "2",
    "timezone": "America/Toronto",
    "description": "SA360 Revenue and Conversion report for Agency/Advertiser",
    "offset": 0,
    "lookback": 0,
    "notifier": {
        "topic": "postprocessor",
        "message": "conversion_and_revenue"
    },
    "agencyName": "Agency",
    "AgencyId": "00000000000000000",
    "advertiserName": "Advertiser",
    "AdvertiserId": "00000000000000000",
    "ConversionMetric": {
        "type": "savedColumnName",
        "value": "[Conv] Checkout + Purchasers"
    },
    "RevenueMetric": {
        "type": "columnName",
        "value": "dfaRevenueCrossEnv"
    }
  }
]
```

In the above example, this will be a report for the tdate range of `today` only, with two columns - one a custom column defined by the advertiser and one a standard SA360 column.


**Process**
1. Create a file named `[report].validate` containing the defintion as described
   above
2. Copy to the `sa360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. The completed file will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again. This indicates a successful completion.
5. The validation results wil lbe placed in a file called `[report].validate-validation.csv`

```
$ gsutil cp convresions_and_revenue.validate gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/conversions_and_revenue.validate.processed
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/conversions_and_revenue.validate-validation.csv
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/conversions_and_revenue.validate-validation.csv
"agency","advertiser","conversionMetric","revenueMetric"
"00000000000000000","00000000000000000","valid","valid"
"00000000000000000","00000000000000001","valid",""
"00000000000000000","00000000000000002","valid",""
"00000000000000000","00000000000000003","valid",""
```

The csv file shows you which columns are valid and which are not. `valid` means it is, `invalid` means it is not and `blank` indicates that that column has no definition.

### Install

This accepts the same format list of `json` objects as in validate, and creates
scheduled jobs for each of the valid ones. The jobs are immediately enabled, and
will run at their next scheduled time.

If the same install file is uploaded twice, the system will only change jobs
that are different and upload new.

The `json` format is identical to the one described above in the `validate` action.

**Process**
1. Create a file named `[report].install` containing the defintion as described
   above
2. Copy to the `sa360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. The completed file will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again. This indicates a successful completion.
5. The validation results wil lbe placed in a file called `[report].install.results`

Source `json`
```

```

```
$ gsutil cp convresions_and_revenue.install gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/conversions_and_revenue.install.processed
gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/conversions_and_revenue.install.results
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-sa360-manager/conversions_and_revenue.install.results
conversions_and_revenue_00000000000000000_00000000000000001 - Valid and installed.
conversions_and_revenue_00000000000000000_00000000000000002 - Valid and installed.
conversions_and_revenue_00000000000000000_00000000000000004 - Valid and installed.
conversions_and_revenue_00000000000000000_00000000000000003 - Valid and installed.```
