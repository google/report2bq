# Report2BQ  Auth From The CLI

## Prerequisites
* A clone of the report2bq repository from GitHub (https://github.com/google/report2bq)
* A working installation of Python (v3.9 preferred, but v3.7 as a minimum)
* A working installation of pip/pip3

## Setting up the environment

### Install the requirements
You can install the requirements into either a Python virtual environment
(recommended) or into the core Python environment. The `venv` method is
recommended as it doesn't pollute the core environment and keeps Python apps
separate.

1. Create a virtual environment, or jump to [step 4](#step4) if you wish to install directly into the master Python environment.
    1. Change into the application directory
    `cd application`
    1. Create the venv
    `python -m virtualenv report2bq-env`

2. Now activate the new virtual environment with:
`source report2bq-env/bin/activate`
Your shell prompt should alter to read
`(report2bq-env) $`

3. [](#step4) You can now install the requirements.
`pip3 install -r requirements.txt`
Once complete you have a valid Python environment with all the Python libraries needed for Report2BQ to run.

### Setting up the `create_token.py` file

* Have a look at `cli/create_token.py` in your favourite editor. Line 19 shows where the secrets file should be located. By default it is found in a subdirectory off the main `application` directory called `secrets`, and is the `client_secrets.json` file you will have created as part of the main installation. At this point it cannot be accessed from Cloud Storage, so you will have to create this location and copy the file into it.

* Once the file is ready, you can set up your local environment and run the OAuth generator.

* To run the token generator, the command line (in an activated virtual environment, as set up above), the command (in *nix)  is: \
    ```PYTHONPATH=`pwd` python cli/create_token.py```
This will present you with a prompt reading
`http://0.0.0.0:8080/`

* Click this link. A browser window should open, alloing you to go through the authentication process.

* Follow the process through to the end, and you will see a simple text-based page reading:
`Your JSON Token has been stored in user_token.json.<p/>You may now end the Python process.`

* You can now return to the command line and end the python process.

* Stored in the current directory will be a file called `user_token.json`. This should now be copied to the Report2BQ tokens bucket. As a reminder, this should be called `<project name>-report2bq-tokens`, and tagged with your email address. A sample command line to do this would be:
`gsutil cp user_token.json gs://<project name>-report2bq-tokens/<my_email>_user_token.json`

You can (and should) now delete the file `user_token.json` from the current directory.
