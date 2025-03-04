Contributing to dCache
======

**dCache** uses the linux kernel model where git is not only source repository, but also the way to track contributions and copyrights.

This page intends to summarize basic information of interest for development, including helpful links to repositories and tools, but will also be used to document our workflows and link to the meeting notes. External contributions to the dCache project are welcome!

## Table of Contents

- [Contributing a patch](#contributing-a-patch)
- [How we use git](#how-we-use-git)
- [Code style](#code-style)
- [Testing](#testing)
- [What to work on](#what-to-work-on)
- [External contribution formalities](#external-contribution-formalities)


## Contributing a Patch

In addition to the master branch, several production branches are maintained at any point in time following the dCache release policy, including the lastest feature releases (x.**0**, x.**1**) up to the newest golden release (x.**2**). A new feature is usually only added to the master branch, while bug fixes are commonly first committed to master and then backported to supported target release branches.

In order to add a patch to the dCache codebase, external contributors must submit a **pull request**, which is then reviewed and eventually merged by development team members. This patch must have a "Signed-off-by" line as detailed [below](#external-contribution-formalities).

Core team members use the Review Board software to inspect and discuss changes, after which they are allowed to commit their patch directly onto the master branch.

In order to create a bug fix for an older, supported version, the target commit is usually cherry-picked from master onto a new branch based on the release version branch one wants to backport to, then a pull-request is created. These pull requests are inspected and merged once a week by someone from the core development team who is responsible for bug fix releases that week.

## How We Use Git

We like our git history clean, which is why the author of a commit should **rebase** against the target branch before committing or creating a pull request so as not to create an extra merge commit. If your branch contains multiple patches, please **squash** them first unless there is a good reason to keep them separate.

## Code Style

We use an adapted version of the [`Google style guide for Java`](https://github.com/google/styleguide) that can be found in the [root of this project for IntelliJ](https://github.com/dCache/dcache/blob/master/intellij-java-google-dcache-style.xml).
The used reformatting involves optimization of imports (reordering), application of all syntactical sugar settings, but does not include code rearrangement (fields, methods, classes) or code cleanup for existing code. Reformatting should be applied to the changed code before submitting a patch.

## Testing

Testing is an important aspect of software quality. We use **JUnit** for functional testing, **SpotBugs** for static code analysis and the **Robot** framework for black-box integration testing. Ideally, please try to add tests when adding a new feature or changing existing code that is not yet covered.

When committing a patch to the GitHub repository, a continuous integration process is triggered that automatically builds dCache and runs this test suite to catch regression errors, giving feedback on the commit. This workflow is increasingly mirrored at and moved to our GitLab instance at DESY, where we are testing and deploying in kubernetes.

## What to Work On

There are three main areas that are addressed on on a regular basis. The first one is bug fixing – users open tickets or we discover issues ourselves that need to be corrected and backported. These issues are usually reported either via opening a GitHub issue, creating a ticket via our support(at)dcache.org mailing list or writing directly to our dev(at)dcache.org mailing list.

The second area is continual software maintenance, which includes keeping the used libraries and general code base up to date, but also experimenting with more modern approaches and frameworks for existing functionalities.

Lastly, dCache is enriched with new capabilities. These may be based on feature requests, for example via GitHub or discussions with users, but are also often motivated by the general roadmap of the dCache project. These goals that we set ourselves include extending functionality that certain components already have to other ones, anticipating upcoming developments of relevance for dCache and in general striving to make the lives of admins easier.

## External Contribution Formalities

Each externally submitted patch must have a "Signed-off-by" line.  Patches without
this line will not be accepted.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below:
```

    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
         have the right to submit it under the open source license
         indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.

```
then you just add a line saying ( git commit -s )

    Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)
