# Guide for Contributors

dCache View is built with openness in mind, and the dCache team encourage anyone 
in the community to contribute. Here are the ways you can contribute:

1. [Request a feature](#Request-a-feature)
2. [Filing Issues](#Filing-Issues)
3. [Submitting Pull Requests](#Submitting-Pull-Requests)

### Request a feature

**If you are requesting for a new feature**, click [here](https://github.com/dCache/dcache-view/issues/new), 
fill the form and _make sure you add a "feature" label_. To add the label, 
click *Labels* on the right hand side of your screen and select _feature_. 
Please provide a very clear description of the feature in details.

### Filing Issues

**If you are filing an issue to report a bug**, please provide:

 1. **A clear description of the bug and related expectations.** Consider using 
 the following example template for reporting a bug:
 ```markdown
 The user icon or image was never shown.

 ## Expected outcome

 The user image should be shown inside the user profile page.

 ## Actual outcome

 The busy loading icon is shown where the user image should be.

 ## Steps to reproduce

 1. Authenticate using username and password.
 2. Go to the user profile page.
 ```
 2. **A list of browsers where the problem occurs.** This can be skipped if the 
 problem is the same across all browsers.

### Submitting Pull Requests

**Before creating a pull request**, please make sure that an issue exists for the 
corresponding change in the pull request that you intend to make. _If an issue does 
not exist, please create one per the guidelines above_. The goal is to discuss the 
design and necessity of the proposed change with dCache team and community before 
diving into a pull request.

When submitting pull requests, please make sure your commit message contains enough 
contextual information. Please provide the following:

 1. **Short summary of the changes** - This should be fewer than 60 characters or 
 less and it must be in a single line. Start the line with 'Fix', 'Add', 'Change'
 instead of 'Fixed', 'Added', 'Changed'.
 
 2. **Motivation** - Explain the reason for this patch. What problem are you 
 trying to solve or what feature are trying to add. Basically, this section 
 should answer the question: what is the motivation behind this patch?
  
 3. **Modification** - Describe the changes you've made, what you've added 
 or modified.
 
 4. **Result** - After the changes, what will change. Explain how the 
 modifications you just described will solve the initial problem.
 
 5. **Target** branch - This is the branch that is targeted with this pull 
 request. This is most likely going to be the *master* branch.
 
 6. **Request** branch - if other branches have the same issue you are fixing 
 in the target branch, list them here and make a pull request to those branches
 too.
 
 7. **Fixes** - A reference to the corresponding issue or issues that will 
 be closed by the pull request. Please refer to these issues in the pull 
 request description using the following syntax:
  
        Fixes: https://github.com/dCache/dcache-view/issues/135

 8. **Signed-off-by** - Each submitted patch must have a "Signed-off-by" line. 
 Patches without this line will not be accepted. The sign-off is a simple line 
 at the end of the explanation for the patch, which certifies that you wrote 
 it or otherwise have the right to pass it on as an open-source patch. The rules 
 are pretty simple: if you can certify the below:

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

Here is a sample of a commit message

 ```markdown
dcache-view (user-profile): make gravatar optional

Motivation:

If a user account have an email associated with it, a request 
is sent to check if the email is registered with the Gravatar, 
and if it is, the image from gravatar is used by dcache-view 
as the user profile. 

Since this is the current default behaviour of dcache-view, 
some sites are not happy with this and the preferred behaviour 
will to make gravatar optional for users.

Modification:

1. properly remove all node inside the user profile section.
2. add a checkbox in the login form. This enable users to 
    choose whether to use a gravatar or not. By default, 
    the checkbox is unchecked. This means that users will 
    have to specifically check the checkbox to indicate that
    dcache-view can make a request to the Gravatar
3. adjust the user-image element
4. update user-profile and user-profile-dropdown element to 
    use the adjusted user-image element.
5. add a section in the user-profile for user to see and sel- 
    ect the preferred user profile image. There are only two
    options (identicon and gravatar) available at this time.

Result:

User can now opt-in or out of gravatar.

Target: master
Request: 1.5
Request: 1.4
Fixes: https://github.com/dCache/dcache-view/issues/135
Signed-off-by: Random J Developer <random@developer.example.org>
 ```
 
### References

 - https://stevegury.github.io/2014-05-09/writing-good-commit-message.html
