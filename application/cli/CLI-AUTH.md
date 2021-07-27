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
    `virtualenv --python=python3 report2bq-env`
    If this fails with an error like this:
    ```
    $ virtualenv --python=python3 report2bq-env
    -bash: /usr/local/bin/virtualenv: No such file or directory
    ```

    ... then you need to install the `virtualenv` package first.

      * To do this, you will need to do a `pip` install of the package. Installing it as privileged is a good
    idea so:
    `sudo pip3 install virtualenv`
    is the command you will need. You should see something like this:
```
$ sudo pip3 install virtualenv
Collecting virtualenv
  Downloading https://files.pythonhosted.org/packages/bc/c1/03d8fe7a20b4b2546037c36e5e0f6080522d36421fbbf4d91211fadac150/virtualenv-20.6.0-py2.py3-none-any.whl (5.3MB)
    100% |████████████████████████████████| 5.3MB 237kB/s
Requirement already satisfied: importlib-metadata>=0.12; python_version < "3.8" in /usr/local/lib/python3.7/dist-packages (from virtualenv) (4.6.1)
Requirement already satisfied: filelock<4,>=3.0.0 in /usr/local/lib/python3.7/dist-packages (from virtualenv) (3.0.12)
Requirement already satisfied: platformdirs<3,>=2 in /usr/local/lib/python3.7/dist-packages (from virtualenv) (2.0.2)
Requirement already satisfied: backports.entry-points-selectable>=1.0.4 in /usr/local/lib/python3.7/dist-packages (from virtualenv) (1.1.0)
Requirement already satisfied: six<2,>=1.9.0 in /usr/local/lib/python3.7/dist-packages (from virtualenv) (1.16.0)
Requirement already satisfied: distlib<1,>=0.3.1 in /usr/local/lib/python3.7/dist-packages (from virtualenv) (0.3.2)
Requirement already satisfied: zipp>=0.5 in /usr/local/lib/python3.7/dist-packages (from importlib-metadata>=0.12; python_version < "3.8"->virtualenv) (3.5.0)
Requirement already satisfied: typing-extensions>=3.6.4; python_version < "3.8" in /usr/local/lib/python3.7/dist-packages (from importlib-metadata>=0.12; python_version < "3.8"->virtualenv) (3.10.0.0)
Installing collected packages: virtualenv
Successfully installed virtualenv-20.6.0
```

Now repeat step 2, and you should see something like this:
```
$ virtualenv --python=python3 report2bq-env
created virtual environment CPython3.7.3.final.0-64 in 595ms
  creator CPython3Posix(dest=/home/davidharcombe/report2bq/application/report2bq-env, clear=False, no_vcs_ignore=False, global=False)
  seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/home/davidharcombe/.local/share/virtualenv)
    added seed packages: pip==21.1.2, setuptools==57.0.0, wheel==0.36.2
  activators BashActivator,CShellActivator,FishActivator,PowerShellActivator,PythonActivator,XonshActivator
$
```
3. Now activate the new virtual environment with:
`source report2bq-env/bin/activate`
Your shell prompt should alter to read
`(report2bq-env) $`

4. [](#step4) You can now install the requirements.
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
