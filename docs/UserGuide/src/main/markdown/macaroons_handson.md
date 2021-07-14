Macaroon Usage Examples
==================================

For the general introduction, see [Macaroons](macaroons.md)

### Hands-on examples on Macaroons with dCache

Macaroons can be generated and used on a HTTP level. In principle, most HTTP clients like `curl` or HTTP libraries should be usable to generate and use macaroons, when header information can be managed.

## Good to Know

#### Environment Variables

##### Grid CA Certificates

Since the Grid infrasturcture uses separate certificate chains than the web, you have to explicitly point your tools to the Grid CA certificates. E.g., for `curl` use `--capath ${CAPATH} ` where `$CAPATH` might point to

```export CAPATH="/cvmfs/grid.cern.ch/etc/grid-security/certificates"```

##### User Proxy/Certificate Envvars

A user needs to authenticate against a dCache instance to request an authorization macaroon. In the following, we will use a Grid user proxy for authz. Generate a valid user proxy for a mapped user, who is authorized to read/write paths on your instance.

For easier handling, we ensure that the user proxy is set in a `${X509_USER_PROXY}` environment variable

    export X509_USER_PROXY="/tmp/x509up_u##UID##"`

Necessary flags in curl to use the Grid user proxy for authentication are

    curl --key ${X509_USER_PROXY} --cert ${X509_USER_PROXY} --cacert ${X509_USER_PROXY} ...


#### curl vs. HTTP redirects
`curl` does not follows HTTP redirects like 301 or 302. Use curl with flags `--location`/`-L`, else a curl HTTP request going to a door for a file might not follow the door's redirect to the actual pool.


## Macaroon Requests

### Example Macaroon Request

Basic request to request a full-power Macaroon with a valid Grid proxy

```
> curl [--include,--fail] --location --key ${X509_USER_PROXY}  --cert ${X509_USER_PROXY}  --cacert ${X509_USER_PROXY}  --capath ${CAPTAH} -X POST -H 'Content-Type: application/macaroon-request' https://dcache-se-doma.desy.de:2880
HTTP/1.1 200 OK
Date: Mon, 29 Jul 2019 10:04:38 GMT
Server: dCache/5.2.1
Content-Type: application/json
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PUT, PROPFIND
Access-Control-Allow-Headers: Content-Type, Authorization
Content-Length: 1082
 
{
"macaroon": "MDAxY2xvY2F0...",
"uri": {
"targetWithMacaroon": "https://dcache-se-doma.desy.de:2880/?authz=MDAxY2xvY2F0aW9uIE9wd....",
"baseWithMacaroon": "https://dcache-se-doma.desy.de:2880/?authz=MDAxY2xvY2F0....",
"target": "https://dcache-se-doma.desy.de:2880/",
"base": "https://dcache-se-doma.desy.de:2880/"
}
}
```

For easier handling in the following, export the macaroon in an environment variable

```export MACAROON="MDAxY2xvY2F0aW9uIE9wd..."```

### Macaoon limited in time and on the path

Generate a macaroon limited to list and download from a specific directory and is valid for 1 day and 12 hours. The caveats have top be put into the request's header. For curl

```
> curl [--include,--fail] -L --key $X509_USER_PROXY  --cert ${X509_USER_PROXY}  --cacert ${X509_USER_PROXY}  --capath ${CAPATH}  -X POST  -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:DOWNLOAD,LIST"],"validity": "P1DT12H"}' https://dcache-se-doma.desy.de:2880/path/allowed/to/access
```

See the [ISO 8061 documentaion](https://en.wikipedia.org/wiki/ISO_8601#Durations) on the syntax for durations .

At maximum, a macaroon can be valid for one week.

### Listing & Reading

#### Listing a Directory

Listing a directory content via the PROFIND WebDAV extension to HTTP. Put the previously requested macaroon into the request's header. Here, we pipe the resulting info page from dCache with the directory content to `xmllint` for better readability on the command line

```
> curl [--include,--fail] -L --capath ${CAPATH} -X PROPFIND -H "Depth: 1" -H "Authorization: Bearer ${MACAROON}" https://dcache-se-doma.desy.de:2880/path/allowed/to/access | xmllint --format -
```

Note, that we had to give two header fields separately, one for the listing depth and one containing the macaroon.

#### Downloading a File

Get a file with HTTP GET

```
 curl [--include,--fail] -L --capath ${CAPATH} -X GET -H "Authorization: Bearer ${MACAROON}" https://dcache-se-doma.desy.de:2880/path/allowed/to/access/file.foo --output /tmp/output.foo
```

### Changing the root Directory

With the 'root' caveat, the base path can be limited to a subtree of the namespace. I.e., You can generate a macaroon allowing access to files living under `/path/allowed/to/access/` - and limit the visible namespace to the macaroon bearer to the same *sub*directory

Request a macaroon allowing for listing and downloading files under `/path/allowed/to/access/` with a lifetime of 1.5 days **and** the root direcory constraint to the same subdirectory
```
> curl [--include,--fail] -L --key $X509_USER_PROXY  --cert ${X509_USER_PROXY}  --cacert ${X509_USER_PROXY} --capath ${CAPATH}  -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:DOWNLOAD,LIST","root:/path/allowed/to/access/"],"validity": "P1DT12H"}' https://dcache-se-doma.desy.de:2880/path/allowed/to/access/
```

For the bearer of the resulting macaroon, listing of the root directory content will then be put into the afromentioned path of the namespace 

```
> curl [--include,--fail] -L--capath ${CAPATH} -X PROPFIND -H "Depth: 1" -H "Authorization: Bearer ${MACAROON}" https://dcache-se-doma.desy.de:2880/
```

### Limiting the path

Similarly, one can generate a macaroon limiting the bearer's path to a specific path, i.e., only the given file/path will be available to the macaroon bearer. 

```
> curl [--include,--fail] -L  --key $X509_USER_PROXY  --cert ${X509_USER_PROXY}  --cacert ${X509_USER_PROXY} --capath ${CAPATH} -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:DOWNLOAD,LIST","path:/path/allowed/to/access/file.foo"],"validity": "P1DT12H"}' https://dcache-se-doma.desy.de:2880
```

or

```
> curl [--include,--fail] -L  --key $X509_USER_PROXY  --cert ${X509_USER_PROXY}  --cacert ${X509_USER_PROXY} --capath ${CAPATH} -X POST -H 'Content-Type: application/macaroon-request' -d '{"caveats": ["activity:DOWNLOAD,LIST"],"validity": "P1DT12H"}' https://dcache-se-doma.desy.de:2880/path/allowed/to/access/file.foo
```

### More Caveats

#### IP Range

To limit for example the IP range to the DESY subnet, add a caveat to the header

```
curl ... -d '{"caveats": ["ip:2001:638:700::/48","ip:131.169.0.0/16"],"validity":...} ...
```


#### Limiting Writing/Upload

For a write-only dropbox macaroon, that would allow the bearer to write **only** onto `https://dcache-se-doma.desy.de:2880/path/allowed/to/access/file.foo`

```
curl ...  -d '{"caveats": ["activity:UPLOAD"],"validity":...}' https://dcache-se-doma.desy.de:2880/path/allowed/to/access/file.foo
```
(to allow the macaroon bearer to also list and read the path, do not forget the `DOWNLOAD` & `LIST` caveats)


##### File Upload/HTTP PUT

To write onto the path as the macaroon bearer

```
curl --location --capath ${CAPATH}  -T /path/to/file/to/be/uploaded https://dcache-se-dome.desy.de:2880/path/allowed/to/access/file.foo?authz=${MACAROON}
```

Optionally, one could use also the root caveat and constraint the root pathfor the bearer, so that one would not need to know the full path and probably just write to the remapped `/`

#### Directory Creation

Create a macaroon, that allows the bearer to create subdirectories

```
curl ... -d '{"caveats": ["activity:MANAGE,LIST"],"validity": ...}' https://dcache-se-dome.desy.de:2880/path/allowed/to/access/
```

and as bearer create a new sub-directory with the MKCOL method in WebDAV

```
curl --capath ${CAPATH} -X MKCOL https://dcache-se-doma.desy.de:2880/path/allowed/to/access/mynew.d -H "Authorization: Bearer ${MACAROON}"
```

#### Deletion

To allow a macaroon bearer the deletion of files etc., apply the `DELETE` caveat during creation

```
{"caveats": ["activity:DELETE"],"validity": ...}
```

The corresponding method/curl request is `-X DELETE` - use with care.

---

## Macaroon I/O in ROOT

Since the HTTP libraries used by [ROOT](https://root.cern/) support all the I/O methods for streaming read/writes over HTTP, macaroons can in principle be used directly. For example, to read a ntuple root file from HTTP authorized with a macaroon and write a plot locally

```
export CAPATH=...
export INMACAROON="https://dcache-se-doma.desy.de:2880/https://dcache-se-doma.desy.de:2880/path/allowed/to/access/myfootuple.root?authz=MDA0MWxvY..."
export OUTMACAROON="/tmp/macaroonio_out.png"
```

```
#include "TH1.h"
#include "TList.h"
#include "TCanvas.h"
#include "TStyle.h"
#include "TSystem.h"
 
void macaroonio() {
  const char * INMACAROON = gSystem->Getenv("INMACAROON");
  const char * OUTMACAROON = gSystem->Getenv("OUTMACAROON");
  TFile *inFile = TFile::Open(INMACAROON);
  TH1D *h1 = new TH1D("h1","Hello Macaroon",200,5000,7000);
  TCanvas * c1 = new TCanvas("c1", "c1", 900, 600);
  c1->cd();
  TObject * inTree = inFile->Get("DecayTree");
  inTree->Draw("Bplus_MM>>h1");
  c1->SaveAs(OUTMACAROON);
}
```