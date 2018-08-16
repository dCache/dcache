CHAPTER 17. dCache WEBADMIN INTERFACE
=====================================

Table of contents
-----------------

[Installation](#installation)

This part describes how to configure the `webadmin` service which runs inside the `httpdDomain` and offers additional features to admins like sending admin-commands equal to those of admin interface (CLI) to chosen cells or displaying billing plots.

For authentication and authorisation it offers usage of username/password (currently the kpwd-Plugin) or grid certificate talking to `gPlazma2`. It also offers a non-authenticated read-only mode.

If you are logged in as admin it is possible to send a command to multiple pools or a whole poolgroup in one go. It is even possible to send a command to any dCache cell. Also, there is information like their size, their id and used space on linkgroups and spacetokens available.

From the technical point of view the `webadmin` service uses a Jetty-Server which is embedded in an ordinary `httpd` cell. It is using apache-wicket (a webfrontend-framework) and YAML (a CSS-Template Framework).



INSTALLATION
============

For the authenticated mode a configured gPlazma is required (see also [the section called “gPlazma config example to work with authenticated webadmin”](config-gplazma.md#gplazma-config-example-to-work-with-authenticated-webadmin). The user may either authenticate by presenting his grid certificate or by entering a valid username/password combination. This way it is possible to login even if the user does not have a grid certificate. For a non-authenticated `webadmin` service you just need to start the `httpd` service.

For the authenticated mode using a grid certificate the host certificate has to be imported into the dCache-keystore. In the grid world host certificates are usually signed by national Grid-CAs. Refer to the documentation provided by the Grid-CA to find out how to request a certificate. To import them into the dCache-keystore use this command:

    [root] # dcache import hostcert  

Now you have to initialise your truststore (this is the certificate-store used for the SSL connections) by using this command:

    [root] # dcache import cacerts

The `webadmin` service uses the same truststore as `webdav` service, so you can skip this step if you have `webdav` configured with SSL.

The default instance name is the name of the host which runs the httpdDomain and the default http port number is `2288` (this is the default port number of the httpd service). Now you should be able to have a read-only access to the webpage http://example.com:2288/webadmin.

In the following example we will enable the authenticated mode.


Example:

    [httpdDomain]
          authenticated=true

The most important value is `httpd.authz.admin-gid`, because it configures who is allowed to alter dCache behaviour, which certainly should not be everyone:

    # # When a user has this GID he can become an admin for the webadmin interface #
    httpd.authz.admin-gid=0

To see all webadmin specific property values have a look at **/usr/share/dcache/defaults/httpd.properties**.

For information on CELL-GPLAZMA configuration have a look at [ Chapter 10, Authorization in dCache](config-gplazma.md) and for a special example [the section called “gPlazma config example to work with authenticated webadmin”](config-gplazma.md#gplazma-config-example-to-work-with-authenticated-webadmin).

  [???]: #cf-gplazma-webadmin-example
  [1]: #cf-gplazma
