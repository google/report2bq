# Report2BQ Authentication and Administration Interface

* Author: David Harcombe (davidharcombe@google.com)
* Type: Open source
* Status: Production

Report2BQ is a scalable, Cloud Functions-based solution to run and fetch
reports from CM and DV360, web-download format reports from SA360 and also
run ADH reports on a schedule.
The entire system has a simple install script, install scripts for setting
up fetchers or runners and a minimal amount of manual actions to be done.

## INSTALLATION GUIDE

### Prerequisites

* A GCP Project in which the current user has Admin access
* The latest version of the Google Cloud SDK and all dependencies, specifically
  the CLI tool
* A checked out copy of the GitHub repo
* An active, valid API Key for the GCP project (steps #3 & #4 of the main installation guide)

### Steps

1. From the root of the cloned repository, change into the `appengine` subdirectory. 
This is where the Administration module is based.

1. Edit the `app.yaml` file in your favourite text editor.  
Modify line 5 (`API_KEY: ""`) and copy/paste the API key from the Credentials page into the API KEY between the quotation marks.  
Save and exit.
****Add screenshot of API Key
**Create jobs**

1. Deploy the appengine instance:  
`gcloud app deploy --project <YOUR PROJECT>`  
When it finishes, it will show you something like this: \
```
Services to deploy:

descriptor:      [/home/davidharcombe/cse/report2bq/appengine/app.yaml]
source:          [/home/davidharcombe/cse/report2bq/appengine]
target project:  [<YOUR PROJECT>]
target service:  [default]
target version:  [20200706t104653]
target url:      [<YOUR APPENGINE URL>]


Do you want to continue (Y/n)?  

Beginning deployment of service [default]...
╔════════════════════════════════════════════════════════════╗
╠═ Uploading 21 files to Google Cloud Storage               ═╣
╚════════════════════════════════════════════════════════════╝
File upload done.
Updating service [default]...done.                                                                                                                                                                                
Setting traffic split for service [default]...done.                                                                                                                                                               
Deployed service [default] to [<YOUR APPENGINE URL>]
```

1. While this is deploying, we now want to secure the appengine server. So let's go do that.

1. Navigate to the IAM & Admin > Identity Aware Proxy section in the Google Cloud Console  
![](screenshots/1_IAP-enable.png)

1. Activate the IAP (if it is not already active). This will take a couple of minutes - hopefully enough time for the appengine instance to have deployed successfully. If not, just let step #3 finish.  

1. **Bookmark the url of your app engine app** (listed in the "Published" column on the "Identity-Aware Proxy" page of the cloud console). You will need it in later steps and it is used to Auth users, including yourself.
1. You will probably need to configure the "OAuth consent screen", these are the steps:
   
   1. Go to API & Services

   1. *If you have not created an OAuth consent screen set one up (see instructions in [../README.md#Authentication](../README.md#Authentication)), otherwise edit the existing one.* Select OAuth Consent Screen in the menu options.
       * The authorised domains to include:  
         * The url of your app engine app

2. When active, ensure that IAP on "App Engine app" is switched on. The appengine uses IAP to ensure you can control who has access - and to get the logged in user's details.
 
3. Now lock it down. You do this by:

    - Selecting the `App Engine app` checkbox on the left to open the info panel.
    - Clicking the `Add member` button, and insert the user(s) (`davidharcombe@google.com` for example) or domains (`google.com`) you want to have access.
    - Selecting the role Cloud IAP > IAP-secured Web App User
    - Clicking `Save`  
    ![](screenshots/2_IAP-configure.png)

4. Allow the newly deployed appengine access to complete the OAuth authentication.
   - Go to the OAuth consent screen at APIs & Services -> Credentials oAuth 2.0 Client IDS "Report2BQ oAuth Client Id"->Edit (pencil icon on the right)
   - `<YOUR APPENGINE URL>` to the 'Authorized Javascript Origins'.
   - Add `<YOUR APPENGINE URL>/oauth-complete` and `<YOUR APPENGINE URL>` to the 'Authorized Redirect URIs'.
   - Save