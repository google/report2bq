# GA360 MANAGER OPERATION

* Author: David Harcombe (davidharcombe@google.com)
* Type: Open source
* Status: Production

## OVERVIEW

The GA360 Manager is a way of adding and validating new GA360 Dynamic reports
and runners without the need to run shell scripts. Everything is performed
using json files stored in a specific GCS bucket. All the user needs is access
to this bucket and their OAuth token registered with Report2BQ.

## PREREQUISITES

1. An up to date version of Report2BQ installed and functioning in a Google
Cloud Project.
1. Access to the `[project]-report2bq-ga360-manager` bucket in Cloud Storage.
   - `[project]` should be replaced with your project name.
   - This bucket will be created by the installer, but if it isn't, you'll
     need to create it.

## OPERATIONS

Please note, all the examples use the `gsutil` command line to handle any Cloud
Storage operations. Using the browser to drop files into the bucket is equally
efficacious, but would require more screenshots.

### List

This will list all available (defined) GA360 dynamic reports along with all
their currently defined scheduled runners.

1. Create a file (0 bytes is fine) called `report2bq.list`.
2. Copy to the `ga360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. Check the `report_list.results` file.
5. The completed action will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again.

#### Example:
```
$ touch report2bq.list
$ gsutil cp report2bq.list gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/report2bq.list.processed
gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/report_list.results
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/report_list.results
ga360_report
  ga360_report_0027342
  ga360_report_0027342
  ga360_report_0028310
  ga360_report_0028644
  ga360_report_1449600
  ga360_report_1469687
  ga360_report_0002096
...
```

### Show

This will extract the `json` definition of the named report from Firestore and
store it as a file.

1. Create a file (0 bytes is fine) named `[report].show`.
2. Copy to the `ga360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. Check the `[report].results` file.
5. The completed action will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again.

#### Example:
```
$ touch ga360_report.show
$ gsutil cp ga360_report.show gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/ga360_report.results
gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/ga360_report.show.processed
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/ga360_report.results
{
  "dateRanges": [
    {
      "startDate": "",
      "endDate": ""
    }
  ],
  "dimensions": [
    {
      "name": "ga:dimension6"
    },
    {
      "name": "ga:dcmLastEventAdvertiser"
    },
    {
      "name": "ga:dcmLastEventSitePlacement"
    },
    {
      "name": "ga:dcmLastEventSitePlacementId"
    }
  ],
  "viewId": "",
  "samplingLevel": "LARGE",
  "metrics": [
    {
      "alias": "Goal 13 Starts",
      "expression": "ga:goal13Starts"
    },
    {
      "alias": "DV360 Cost",
      "expression": "ga:dbmCost"
    },
    {
      "expression": "ga:dcmCost",
      "alias": "CM360 Cost"
    }
  ]
}
```

### Add

This will add a new GA360 dynamic report, based on the supplied `json` file.
The file should be in the following structure:
```
{
  "dateRanges": [
    {
      "startDate": "",
      "endDate": ""
    }
  ],
  "dimensions": [
    {
      "name": "ga:dimension6"
    },
    ...
  ],
  "viewId": "",
  "samplingLevel": "LARGE",
  "metrics": [
    {
      "alias": "Goal 13 Starts",
      "expression": "ga:goal13Starts"
    },
    ...
  ]
}
```

| Property name | Type | Description | Notes |
| --- | --- | --- | --- |
| **viewId** | string | The viewId | This should be blank, as it will be substituted in at run time. |


**A full example report definition**
```
{
  "dateRanges": [
    {
      "startDate": "",
      "endDate": ""
    }
  ],
  "dimensions": [
    {
      "name": "ga:dimension6"
    },
    {
      "name": "ga:dcmLastEventAdvertiser"
    },
    {
      "name": "ga:dcmLastEventSitePlacement"
    },
    {
      "name": "ga:dcmLastEventSitePlacementId"
    }
  ],
  "viewId": "",
  "samplingLevel": "LARGE",
  "metrics": [
    {
      "alias": "Goal 13 Starts",
      "expression": "ga:goal13Starts"
    },
    {
      "alias": "DV360 Cost",
      "expression": "ga:dbmCost"
    },
    {
      "expression": "ga:dcmCost",
      "alias": "CM360 Cost"
    }
  ]
}
```

**Process**
1. Create a file named `[report].add` containing the defintion as described
   above
2. Copy to the `ga360-manager` bucket in Cloud Storage.
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
2. Copy to the `ga360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. Check the `[report].results` file.
5. The completed action will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again. This indicates a successful completion.

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
2. Copy to the `ga360-manager` bucket in Cloud Storage.
3. Wait. The operation usually completes in under 15s.
4. The completed file will be renamed by appending `.processed` to ensure the
manager doesn't simply run it again. This indicates a successful completion.
5. The validation results wil lbe placed in a file called `[report].install.results`

Source `json`
```

```

```
$ gsutil cp convresions_and_revenue.install gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager
$ gsutil ls gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager
gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/conversions_and_revenue.install.processed
gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/conversions_and_revenue.install.results
$ gsutil cat gs://report2bq-zz9-plural-z-alpha-report2bq-ga360-manager/conversions_and_revenue.install.results
conversions_and_revenue_00000000000000000_00000000000000001 - Valid and installed.
conversions_and_revenue_00000000000000000_00000000000000002 - Valid and installed.
conversions_and_revenue_00000000000000000_00000000000000004 - Valid and installed.
conversions_and_revenue_00000000000000000_00000000000000003 - Valid and installed.
```
