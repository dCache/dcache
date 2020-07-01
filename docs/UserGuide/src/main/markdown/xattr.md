Chapter 2. Extended Attributes
==============================

**Table of Contents**

+ [How metadata is stored](#how-metadata-is-stored)
+ [Protocol support](#protocol-support)
+ [Operations](#operations)
+ [Authorisation](#authorisation)

Extended attributes are a way you can store more-or-less arbitrary
information about a file or directory.  This could be useful in
remembering some summary details of a file's contents or the kind of
files stored in a directory.  For example, for a photograph, extended
attributes could be used to store information about the camera used,
the camera's settings when taking the photo or the subject matter.
For data taken from a telescope, extended attributes might store the
region of the sky, the time period when the data was taken and which
frequency ranges or filters used when capturing the data.  More
generally, extended attributes may be used to store external
references, such as the file's reference within some data catalogue.

### How metadata is stored

The information in extended attributes is stored as pairs of
information: a name and a value.  The extended attribute's name
describes what kind of information the attribute is storing while the
extended attribute value holds the precise information for this file
or directory.  For example, a file may have an extended attribute
named `catalogue-id` with a value that is some identifier (perhaps a
number or a URL) within some external data catalogue; e.g.,
`https://catalogue.example.org/file/14324`.

The extended attribute names and values are fairly arbitrary.  Both
extended attribute names and values may be any valid UTF-8 string.
Although dCache will store this information, it makes no attempt to
understand or act on the supplied information.  In particular, the
extended attribute's value may be formatted in whichever way is
appropriate for the context.  For example, the value could be a simple
label, a space- or comma-separated list, a URI, a JSON object, etc.

For any file or directory, the stored extended attribute names must be
unique.  Two operations that refer to the same target (a file or
directory) and the same extended attribute name are referring to the
same extended attribute.  Therefore, it is not possible to create two
distinct extended attributes with the same name.  If you want dCache
to store a collection of similar values (e.g., a list of tags) then
that should be stored as a single extended attribute with a value
containing the multiple values that are encoded into a single extended
attribute value.  This could be as a space- or comma- separated list,
as a JSON or YAML array, or in some other format.

Extended attributes are global and are not stored per user or per
group: if one user assigns a file some extended attribute named `foo`
(with value `bar`, say) then all users will see this extended
attribute.  This means that users of a specific extended attribute
name must have a common understanding of this attribute's semantics;
for example, if a file has an extended attribute `catalogue-id` then
all users that read this attribute must understand in which format the
ID is held (an integer value, a URI, a JSON Object, ...), the
semantics of this value (identifies the file within the catalogue, or
the identifier only catalogue with the file-specific identifer held in
a separate extended attribute), within which catalogue the file is
registered (if not stored explicitly), etc.  The users of this
attribute also need to coordinate how the attribute value is assigned.

### Protocol support

Support for querying and updating extended attributes are available
through various protocols supported by dCache.  The extended
attributes are consistent across multiple protocols: metadata stored
through one protocol is both visible and modifiable through all
protocols.

The NFS door supports RFC 8276, which describes an extension to NFSv4
that allows clients to query and modify extended attributes.  The [NFS
chapter](nfs.md) contains more details.

The frontend door provides roughly similar extended attribute
functionality to the NFS protocol but using an HTTP REST API.  The
[frontend chapter](frontend.md) describes how to work with extended
attributes through this API, with the [discovering
metadata](frontend.md#discovering-metadata) section describing how to
query currently assigned extended attributes and the [managing
extended attributes](frontend.md#managing-extended-attributes) section
describing how to create, modify or remove extended attributes.

Finally, the WebDAV door supports extended attributes by mapping them
to WebDAV properties.  This allows quering current extended
attributes, assigning extended attributes, and removing existing
extended attributes.  The [extended attributes
section](webdav.md#extended-attributes) in the WebDAV chapter has more
details on how this works.

### Operations

Depending on the protocol used, clients may have more sophisticated
options when assigning an extended attribute.  Using the NFS or REST
interfaces, a client can choose how an extended attribute is added or
modified.  In general, there are three modes when modifying a file's
extended attributes: CREATE, MODIFY or EITHER.  In CREATE mode, dCache
will accept an extended attribute assignment only if there is no
existing extended attribute with the same name; if the named extended
attribute already exists then the operation will fail.  In MODIFY
mode, dCache will accept an extended attribute assignment only if
there is already an extended attribute with the same name.  Finally,
in EITHER mode the attribute is created if it does not already exist,
or is updated if it already exists.  The CREATE mode is useful for
assigning identifiers that should not change; MODIFY is useful in
certain circumstances for modifying optional information atomically;
EITHER mode is perhaps the most often used, where information is
always assigned.

Extended attributes may also be removed.  This operation is a distinct
from assigning an extended attribute a new value.  Specifically,
assigning an empty value to an existing extended attribute will not
delete the extended attribute as an empty value is valid.

### Authorisation

Extended attributes do not have separate authorisation information.
Instead, a user is allowed to read, add, modify and remove extended
attributes based on the corresponding file or directory permissions.
If the user can read a file's contents then that user can also read
extended attributes.  If the user can (in principle) modify a file's
contents then that user can modify that file's set of extended
attributes by creating new extended attribute, or by modifying or
removing existing extended attribute.  For directories, the ability to
list a directory allows a user to read a directory's extended
attributes and the ability to create new content (files or
directories) within a directory means that user is able to modify that
directory's set of extended attributes.
