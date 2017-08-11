Series of dCache Frontend Service Tests
=======================================

dCache Frontend Service is a Restful API for dCache. These series of tests can
be only run after the system-test module have been successfully build and
started.

This document contain instructions on the recommended installation procedure for
software behind the test system and how to run the test(s).


### Table of Contents
**[Robot Framework Installation Procedure](#Robot-Framework-Installation-Procedure)**<br>
**[How to Run the test(s)](#How-to-Run-the-test)**<br>


<a name="Robot-Framework-Installation-Procedure"/>

## Robot Framework Installation Procedure

We are using [Robot Framework](http://robotframework.org/) for our test system.
Robot Framework utilizes the keyword-driven testing automation approach. How to
install Robot Framework is well documented
[here](https://github.com/robotframework/robotframework/blob/master/INSTALL.rst).

However, below is the installation procedure I will recommend:

#### Pre-installation

Before installing robot framework, these following must be
installed first:

1. Make sure you have python installed on your system. The latest release of Robot
Framework, that is, version 3.0 will required at lease Python 2.7 and it also support
Python >= 3.0.  Before installing python, please check if it is alread pre-installed
on your system. To check, typed this in your `terminal`:

>     python -V

If you have it installed, you see something like:

```
Python 2.7.10
```

So, if you don't have python on your system, go to https://www.python.org/ to
download the right package and follow the installation instruction for your OS.

2. Next thing to do is to install `pip` (if you don't have it already).
Installation instruction can be found
[here](https://pip.pypa.io/en/stable/installing/). _pip is a package management
system used to install and manage software packages written in Python._

First check if pip is installed on system

>     pip -V

or

>     pip --version

If it is not install, on Linux or macOS run the following command

>     sudo easy_install pip

On Linux or macOS to update

>     pip install -U pip


#### Installation

To install Robot Framework using pip, execute the following command

>     pip install robotframework

Upgrading to the latest version is even very easy

>     pip install --upgrade robotframework

Uninstall

>     pip uninstall robotframework


Check if everything is fine before proceeding

>     robot --version

If robot is install, you should see something like:

```
Robot Framework 3.0 (Python 2.7.10 on linux2)
```

#### Post - Robot Framework Installation

The system-test test for the frontend is using an external library called
`robotframework-requests`.  Hence one needs to install the library dependency
before install the library itself.

Via `pip`, execute the following command one after the other

>     pip install -U requests
>     pip install -U robotframework-requests


<a name="How-to-Run-the-test"/>

## How to Run the test(s)

Once all the necessary dependencies had been installed, now you can run the
test.  In your terminal, change directory to where dcache project source files
is located.

Once system-test is built, the robot test files for the frontend
service can be found in a directory named `frontend` which is located
in:

```
packages/system-test/target/dcache/share/robot
```

Hence, change directory into that directory.


###### For the whole frontend tests:

>     robot frontend


###### For a particular test suite:

Specify the path to the file (that is, the test-suite file):

>     robot  [path_to_the_test_suite_file]

Example
-------

Say the test-suite file path is
`frontend/authentication-test/invalid_login_attempt.robot`;

hence simply run the command below to test it:

>     robot frontend/authentication-test/Suppress_WWW_Authenticate.robot


###### To test a particular test case in a test suite,

>     robot --test [name_of_the_test_case] [path_to_the_test_suite_where_the_test_case_is]

or

>     robot -t [name_of_the_test_case] [path_to_the_test_suite_where_the_test_case_is]

Example:
---------

To test a test case with

`name`: _Response Header Include WWW-Authenticate_

and the test suite file path

`path`: _frontend/authentication-test/Suppress_WWW_Authenticate_ ;

your robot command test will look like this:

```
    robot --test 'Response Header Include WWW-Authenticate' frontend/authentication-test/Suppress_WWW_Authenticate.robot
```


###### For a particular tag:

>     robot --include [space_seperated_tag_name/s]  [path_to_the_test]

By default, the system-test uses default credentials, that is, username and
password. This might not work if it not setup properly. Therefore, to use a
specific username and password, simply do this:

>     robot --variable username:[enter_username_here] --variable password:[enter_password_here] frontend