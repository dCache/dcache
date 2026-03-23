dCache View
===========

dCache View is a web application client for [dCache](https://dcache.org) storage system. 
Basically, it provide an interactive user interface to the end-users of dCache storage 
system, with the aim of making the usage of dCache effortlessly. 

To see how dCache View looks like and to have a feel about the application, try our live 
test machine called [Prometheus](https://prometheus.desy.de:3880). This test machine is 
a small dCache instance running the latest development build of dCache with the latest 
released version of dCache View.

### Table of Contents

1. [Getting Started](#Getting-Started)
    - [Prerequisites](#Prerequisites)
    - [Build](#Build)
    - [Update](#Update/Deployment)
2. [Features](#Features)
    - [List of available Features](docs/features/shipped.md)
    - [Upcoming Features](docs/features/upcoming.md)
3. [How to](#How-To)
    - [make dCache View aware of the OpenID set up](docs/how-tos/open-id.md)
    - [share a file and view a shared file](docs/how-tos/share-file.md)
4. [Versioning](#Versioning)
5. [Contributors](#Contributors)
6. [How to contribute](#How-to-contribute)
7. [License](#License)
8. [Acknowledgments](#Acknowledgments)

## Getting Started

dCache View is part of dCache's [frontend service](https://www.dcache.org/manuals/Book-5.0/config-frontend.shtml). 
Also, it uses the frontend's [RESTful API](https://prometheus.desy.de:3880/api/v1/) for the 
namespace operations and to communication with dCache. In addition to the frontend service, 
dCache View uses the dCache's [webDAV services](https://www.dcache.org/manuals/Book-5.0/config-frontend.shtml) 
for generating macaroons, to perform read and write operation etc. 

A running dCache instance comes with dCache View. For instant, if you are running a 
system-test, all the basic functionality (that will make dCache works out of the box) 
were already set-up for you. It worth mentioning that, the system test have quite a 
few frontend services, which runs on different ports. Hence, you can view dCache View 
at http://localhost:3880/ and https://localhost:3881/. 

__NOTE: You can skip the rest of this part and jump to [how to build](#Build) dCache 
View, if you are running the system-test package.__

However, if you are interested in other packages apart from the system-test, enabling dCache View 
is as simple as starting or adding a frontend service/door to your dCache domain. Say for example, 
you have a single domain called `dCacheDomain`, inside your layout file, which should be located 
at `/etc/dcache/layouts/<name-of-your-layout-file>.conf`. To add the frontend service just add the 
following: `[dCacheDomain/frontend]`. Also don't forget to add the WebDAV door, as pointed out 
earlier dCache View relies on it. Finally, restart your dCache instance. Hence, your layout file 
should look like this:

```text
[dCacheDomain]
.
.
.

[dCacheDomain/webdav]
.
.
.

[dCacheDomain/frontend]

.
.
.
```

By default, dCache View is served from port 3880 but this is configurable 
(see [dCache book](https://www.dcache.org/manuals/book.shtml) for full details on what and how 
to configure all the necessary properties for both webDAV and frontend door).


Ideally, the top of all the supported branches in dCache View [repository](https://github.com/dCache/dcache-view) 
are production ready. If you are brave and want to live on the hedge or (and) fiddle around with 
the source code; you can run the latest version of dCache View but first you need to make 
sure you have [the prerequisites listed here](#Prerequisites) ready. Next, follow the instruction 
described [here on how to build](#Build) dCache View and lastly, deploy it to your system by doing 
what was highlighted [here](#Update/Deployment).

#### Prerequisites

- A running dCache instance (see https://www.dcache.org/manuals/Book-5.0/install.shtml on how to install 
    and start a small dCache instance)
- **Git** - see https://git-scm.com/book/en/v2/Getting-Started-Installing-Git on how to install git.
     Your system might have git already installed, check by running the following command: `git --version` 
     from the *terminal*. If you have it installed (or successful install), the result should look similar to:
     ```
     git version 2.10.1 (Apple Git-78)
     ```
- **Maven** - see https://maven.apache.org/install.html on how to install maven.
    After installation (or before installation to check if you have maven installed already), confirm 
    that this is successful, open a *terminal* and typed `mvn -v`. You should see something like:
    ```
    Apache Maven 3.3.9 (bb52d8502b132ec0a5a3f4c09453c07478323dc5; 2015-11-10T17:41:47+01:00)
    Maven home: /opt/maven-3.3.9
    Java version: 1.8.0_181, vendor: Oracle Corporation
    Java home: /Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre
    Default locale: en_GB, platform encoding: UTF-8
    OS name: "mac os x", version: "10.13.6", arch: "x86_64", family: "mac"
    ```
- Get dCache View source code, use either: 
    ```git
    git clone https://github.com/dCache/dcache-view.git
    ```
    or download the zip file [here](https://github.com/dCache/dcache-view/archive/master.zip)

#### Build

Once you've make sure all the prerequisites requirements are met, open your *terminal* and do 
the following:

- *change directory* to the directory where you forked/downloaded dCache View: 
    ```sbtshell
    cd <path-to-directory>/dcache-view
    ```
- next, *build* with this command
    ```sbtshell
    mvn clean package
    ```

Ensure that the build was successful before you move to the next step, that is, update/deployment. 
To check if it is successful, you should something similar to what is below inside your terminal:

```sbtshell
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 58.926 s
[INFO] Finished at: 2019-01-23T15:41:07+01:00
[INFO] Final Memory: 11M/213M
[INFO] ------------------------------------------------------------------------
```    

#### Update/Deployment

After a successful build, directory name `target` will be created inside `dcache-view` folder. We are 
interested in these following generated files/directories:

    - index.html
    - robots.txt
    - bower_components/
    - elements/
    - favicons/
    - scripts/
    - style/
    
How to update (or deploy) dCache View in (or into) your dCache instance will depends on where it is. 
If it is a locally running dCache instance, it is as simple as copying those files/directories listed 
above to `/usr/share/dcache/dcache-view/` or the equivalent of this path. To copy these files and 
directories from the `target` directory to `/usr/share/dcache/dcache-view/`, in the *terminal* typed:
  ```sbtshell
  rm -rf /usr/share/dcache/dcache-view/* && mv -v <path-to-dir>/dcache-view/target/* /usr/share/dcache/dcache-view/
  ``` 
Remembered, this path: `/usr/share/dcache/dcache-view/` depends on your installation and the package 
you installed. For example, in the system test, this is equivalent to: 
```
<path-to-dcache-directory>/packages/system-test/target/dcache/share/dcache-view/
```

In the case where dCache is running on a remote machine, please use one of the method described 
[here](https://developer.mozilla.org/en-US/docs/Learn/Common_questions/Upload_files_to_a_web_server) 
to deploy or replaced the files listed above with the newly generated ones.

## Features

dCache View comes with many features. 
[Here we've highlighted several of these features that are already shipped](docs/features/shipped.md). 
Also, [a list of planned features and their progress will be provided](docs/features/upcoming.md). 
If you have any suggestions on our features, please submit feedback on our feature requests 
[here](CONTRIBUTING.md#Request-a-feature).

## How To

- [make dCache View aware of the OpenID set up](docs/how-tos/open-id.md)
- [share a file and view a shared file](docs/how-tos/share-file.md)

## Versioning

For the versions available, see the [tags on this repository](https://github.com/dCache/dcache-view/tags) 
for the prebuild and [the nexus]() for the build ones. 

## Contributors

dCache View is part of dCache project, which is a joint venture between Deutsches Elektronen-Synchrotron, 
[DESY](https://www.desy.de/en), Fermi National Accelerator Laboratory, [FNAL](https://www.fnal.gov/) and 
Nordic DataGrid Facility, [NDGF](https://neic.no/nt1/).

## How to contribute

Please read [contributing instruction](CONTRIBUTING.md#Submitting-Pull-Requests) for details on our code of 
conduct, and the process for submitting pull requests to us. Also, if you hit an unknown bug, please 
[check here](CONTRIBUTING.md#Filing-Issues) for instructions on how to report a bug.

## License

The project is licensed under __AGPL v3__ - see the [LICENSE.md](LICENSE.md) file for details


## Acknowledgments

The team thank Onno Zweers from surfSARA for his contributions.
