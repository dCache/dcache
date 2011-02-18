package gplazma.authz.util;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.globus.gsi.bc.X509NameHelper;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.NoAuthorization;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.GssSocket;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;
import org.bouncycastle.asn1.x509.TBSCertificateStructure;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.DERObjectIdentifier;
import org.bouncycastle.asn1.DERString;
import org.opensciencegrid.authz.xacml.common.FQAN;
import org.glite.voms.*;
import org.glite.voms.ac.AttributeCertificate;
import org.glite.voms.ac.VOMSTrustStore;
import org.glite.voms.ac.ACValidator;
import org.glite.voms.ac.ACTrustStore;

import java.net.Socket;
import java.util.*;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CRLException;
import java.io.IOException;
import java.io.File;

import gplazma.authz.AuthorizationException;
import gplazma.authz.AuthorizationController;

/**
 * X509CertUtil.java
 * User: tdh
 * Date: Sep 15, 2008
 * Time: 5:06:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class X509CertUtil {

    public static String default_service_cert          = "/etc/grid-security/hostcert.pem";
    public static String default_service_key           = "/etc/grid-security/hostkey.pem";
    public static String default_trusted_cacerts = "/etc/grid-security/certificates";

    private static PKIStore caTrustStore=null;
    private static VOMSTrustStore vomsTrustStore=null;
    private static ACTrustStore acTrustStore=null;
    private static VOMSValidator vomsValidator=null;
    private static ACValidator acValidator=null;
    private static PKIVerifier pkiVerifier=null;

    private static int REFRESH_TIME_MS=20000;

    public static GSSContext getUserContext(String proxy_cert) throws GSSException {
       return getUserContext(proxy_cert, default_trusted_cacerts);
    }

    public static GSSContext getUserContext(String proxy_cert, String service_trusted_certs) throws GSSException {

        GlobusCredential userCredential;
        try {
            userCredential =new GlobusCredential(proxy_cert, proxy_cert);
        } catch(GlobusCredentialException gce) {
            throw new GSSException(GSSException.NO_CRED , 0,
                    "could not load host globus credentials "+gce.toString());
        }

        GSSCredential cred = new GlobusGSSCredentialImpl(
                userCredential,
                GSSCredential.INITIATE_AND_ACCEPT);
        TrustedCertificates trusted_certs =
                TrustedCertificates.load(service_trusted_certs);
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
                (ExtendedGSSContext) manager.createContext(cred);

        context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES, trusted_certs);

        return context;
    }

    public static Socket getGsiClientSocket(String host, int port, ExtendedGSSContext context) throws Exception {
        Socket clientSocket = GssSocketFactory.getDefault().createSocket(host, port, context);
        ((GssSocket)clientSocket).setWrapMode(GssSocket.GSI_MODE);
        ((GssSocket)clientSocket).setAuthorization(NoAuthorization.getInstance());
        return(clientSocket);
    }

    /**
     * Returns the Globus formatted representation of the
     * subject DN of the specified DN.
     *
     * @param dn the DN
     * @return the Globus formatted representation of the
     *         subject DN.
     */
    public static String toGlobusID(Vector dn) {

        int len = dn.size();
        StringBuffer buf = new StringBuffer();
        for (int i=0;i<len;i++) {
            Vector rdn = (Vector)dn.elementAt(i);
            // checks only first ava entry
            String [] ava = (String[])rdn.elementAt(0);
            buf.append('/').append(ava[0]).append('=').append(ava[1]);
        }
        return buf.toString();
    }

    /**
     * Converts the certificate dn into globus dn representation:
     * 'cn=proxy, o=globus' into '/o=globus/cn=proxy'
     *
     * @param  certDN regural dn
     * @return globus dn representation
     */
    public static String toGlobusDN(String certDN) {
        StringTokenizer tokens = new StringTokenizer(certDN, ",");
        StringBuffer buf = new StringBuffer();
        String token;

        while(tokens.hasMoreTokens()) {
            token = tokens.nextToken().trim();
            buf.insert(0, token);
            buf.insert(0, "/");
        }

        return buf.toString();
    }

    public static String getSubjectFromX509Chain(X509Certificate[] chain, boolean omitEmail) throws Exception {
        String subjectDN;

        TBSCertificateStructure tbsCert = getUserTBSCertFromX509Chain(chain);
        subjectDN = X509NameHelper.toString(tbsCert.getSubject());
        subjectDN = toGlobusString((ASN1Sequence)tbsCert.getSubject().getDERObject(), omitEmail);
        return subjectDN;
    }

    public static TBSCertificateStructure getUserTBSCertFromX509Chain(X509Certificate[] chain) throws Exception {
        TBSCertificateStructure tbsCert=null;
        X509Certificate	clientcert=null;
        for (int i=0; i<chain.length; i++) {
            X509Certificate	testcert = chain[i];
            tbsCert  = BouncyCastleUtil.getTBSCertificateStructure(testcert);
            int certType = BouncyCastleUtil.getCertificateType(tbsCert);
            if (!org.globus.gsi.CertUtil.isImpersonationProxy(certType)) {
                clientcert = chain[i];
                break;
            }
        }

        if(clientcert == null) {
            throw new AuthorizationException("could not find clientcert");
        }

        return tbsCert;
    }

    public static X509Certificate getUserCertFromX509Chain(X509Certificate[] chain) throws Exception {
        TBSCertificateStructure tbsCert=null;
        X509Certificate	clientcert=null;
        for (int i=0; i<chain.length; i++) {
            X509Certificate	testcert = chain[i];
            tbsCert  = BouncyCastleUtil.getTBSCertificateStructure(testcert);
            int certType = BouncyCastleUtil.getCertificateType(tbsCert);
            if (!org.globus.gsi.CertUtil.isImpersonationProxy(certType)) {
                clientcert = chain[i];
                break;
            }
        }

        if(clientcert == null) {
            throw new AuthorizationException("could not find clientcert");
        }

        return clientcert;
    }

    public static String getSubjectX509Issuer(X509Certificate[] chain) throws Exception {
       X509Certificate	clientcert = getUserCertFromX509Chain(chain);
       return getSubjectX509Issuer(clientcert);
    }

    public static String getSubjectX509Issuer(X509Certificate cert) throws Exception {
       return toGlobusDN(cert.getIssuerDN().toString());
    }

    public static Collection<String> getFQANsFromContext(ExtendedGSSContext gssContext, boolean validate) throws AuthorizationException {
        X509Certificate[] chain;
        try {
            chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch (GSSException gsse) {
            throw new AuthorizationException("Could not extract certificate chain from context " + gsse.getMessage() + "\n" + gsse.getCause());
        }
        return getFQANsFromX509Chain(chain, validate);
    }

    public static Collection <String> getFQANsFromContext(ExtendedGSSContext gssContext) throws AuthorizationException {
        X509Certificate[] chain;
        try {
            chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch (GSSException gsse) {
            throw new AuthorizationException("Could not extract certificate chain from context " + gsse.getMessage() + "\n" + gsse.getCause());
        }
        return getFQANsFromX509Chain(chain, false);
    }

    public static Collection <String> getValidatedFQANsFromX509Chain(X509Certificate[] chain) throws AuthorizationException {
        return getFQANsFromX509Chain(chain, true);
    }

    public static Collection <String> getFQANsFromX509Chain(X509Certificate[] chain) throws AuthorizationException {
        return getFQANsFromX509Chain(chain, false);
    }

    public static Collection <String> getFQANsFromX509Chain(X509Certificate[] chain, boolean validate) throws AuthorizationException {
        Collection <String> fqans=null;
        try {
            List listOfAttributes = getVOMSAttributes(chain, validate);
            fqans = getFQANSfromVOMSAttributes(listOfAttributes);
        } catch(AuthorizationException ae ) {
            throw new AuthorizationException(ae.toString());
        } catch(Exception e) {
            throw new AuthorizationException("Could not validate role.");
        }

        return fqans;
    }

    /**
   *  We want to keep different roles but discard subroles. For example,
attribute : /cms/uscms/Role=cmssoft/Capability=NULL
attribute : /cms/uscms/Role=NULL/Capability=NULL
attribute : /cms/Role=NULL/Capability=NULL
attribute : /cms/uscms/Role=cmsprod/Capability=NULL

   should yield the roles

   /cms/uscms/Role=cmssoft/Capability=NULL
   /cms/uscms/Role=cmsprod/Capability=NULL
*/

    public static LinkedHashSet<String> getFQANSfromVOMSAttributes(List listOfAttributes) {
        LinkedHashSet<String> fqans = new LinkedHashSet <String> ();

        Iterator i = listOfAttributes.iterator();
        while (i.hasNext()) {
            VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
            List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
            Iterator j = listOfFqans.iterator();
            while (j.hasNext()) {
                String attr = (String) j.next();
                if(attr.endsWith(AuthorizationController.capnull))
                attr = attr.substring(0, attr.length() - AuthorizationController.capnulllen);
                if(attr.endsWith(AuthorizationController.rolenull))
                attr = attr.substring(0, attr.length() - AuthorizationController.rolenulllen);
                fqans.add(attr);
            }
        }

        return fqans;
    }

    public static AttributeCertificate getAttributeCertificate(X509Certificate[] chain, String fqan) throws AuthorizationException {
        return getVOMSAttribute(chain, fqan).getAC();
    }

    public static VOMSAttribute getVOMSAttribute(X509Certificate[] chain, String fqan) throws AuthorizationException {

        if(fqan.endsWith(AuthorizationController.capnull))
                fqan = fqan.substring(0, fqan.length() - AuthorizationController.capnulllen);
        if(fqan.endsWith(AuthorizationController.rolenull))
                fqan = fqan.substring(0, fqan.length() - AuthorizationController.rolenulllen);

        List listOfAttributes = getVOMSAttributes(chain, false);

        Iterator i = listOfAttributes.iterator();
        while (i.hasNext()) {
            VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
            List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
            Iterator j = listOfFqans.iterator();
            while (j.hasNext()) {
                String attr = (String) j.next();
                String attrtmp=attr;
                if(attrtmp.endsWith(AuthorizationController.capnull))
                    attrtmp = attrtmp.substring(0, attrtmp.length() - AuthorizationController.capnulllen);
                if(attrtmp.endsWith(AuthorizationController.rolenull))
                    attrtmp = attrtmp.substring(0, attrtmp.length() - AuthorizationController.rolenulllen);
                if(attrtmp.equals(fqan)) return vomsAttribute;
            }
        }

        return null;
    }

    public static synchronized List getVOMSAttributes(X509Certificate[] chain, boolean validate) throws AuthorizationException {
        try {
            VOMSValidator validator = getVOMSValidatorInstance();
            validator.setClientChain(chain);
            if(validate) {
                validator.validate();
            } else {
                validator.parse();
            }
            return validator.getVOMSAttributes();
        } catch (IOException ioe) {
            throw new AuthorizationException("Could not read trust stores " + ioe.getMessage() + "\n" + ioe.getCause());
        } catch (CertificateException ce) {
            throw new AuthorizationException("Could not read certificate " + ce.getMessage() + "\n" + ce.getCause());
        } catch (CRLException crle) {
            throw new AuthorizationException("Could not read CRL " + crle.getMessage() + "\n" + crle.getCause());
        }
    }

    public static String parseGroupFromFQAN(String fqan) {
        String group=null;
        if(fqan!=null) {
            group = (new FQAN(fqan)).getGroup();
            StringTokenizer st = new StringTokenizer(group, "/");
            if (st.hasMoreTokens()) {
                group = "/" + st.nextToken();
            }
        }
        return group;
    }

    public static String toGlobusString(ASN1Sequence seq, boolean omitEmail) {
	  if (seq == null) {
	    return null;
	  }

	  Enumeration e = seq.getObjects();
	  StringBuffer buf = new StringBuffer();
        while (e.hasMoreElements()) {
            ASN1Set set = (ASN1Set)e.nextElement();
	    Enumeration ee = set.getObjects();
	    boolean didappend = false;
	    while (ee.hasMoreElements()) {
		ASN1Sequence s = (ASN1Sequence)ee.nextElement();
		DERObjectIdentifier oid = (DERObjectIdentifier)s.getObjectAt(0);
		String sym = (String) X509Name.OIDLookUp.get(oid);
        if (oid.equals(X509Name.EmailAddress) && omitEmail) {
            continue;
        }
        if(!didappend) { buf.append('/'); didappend = true; }
        if (sym == null) {
		    buf.append(oid.getId());
		} else {
		    buf.append(sym);
		}
		buf.append('=');
		buf.append( ((DERString)s.getObjectAt(1)).getString());
		if (ee.hasMoreElements()) {
		    buf.append('+');
		}
	    }
	  }

	  return buf.toString();
    }

    public static synchronized VOMSValidator getVOMSValidatorInstance() throws IOException, CertificateException, CRLException {
        if(vomsValidator!=null) return vomsValidator;
        PKIStore vomsStore=null;
        String vomsDir = System.getProperty( "VOMSDIR" );
        vomsDir = (vomsDir == null ) ? PKIStore.DEFAULT_VOMSDIR : vomsDir;
        File theDir = new File(vomsDir);
        if (theDir.exists() && theDir.isDirectory() && theDir.list().length > 0) {
            vomsStore = new PKIStore(vomsDir, PKIStore.TYPE_VOMSDIR, true);
            vomsStore.rescheduleRefresh(900000);
        }

        PKIStore caStore;
        String caDir = System.getProperty( "CADIR" );
        caDir = (caDir == null) ? PKIStore.DEFAULT_CADIR : caDir;
        caStore = new PKIStore( caDir, PKIStore.TYPE_CADIR, true );
        caStore.rescheduleRefresh(900000);

        vomsValidator = new VOMSValidator(null, new ACValidator(new PKIVerifier(vomsStore, caStore)));
        return vomsValidator;
    }

    public static synchronized ACTrustStore getACTrustStoreInstance() throws IOException, CertificateException, CRLException {
        if(acTrustStore!=null) return acTrustStore;
        acTrustStore = new BasicVOMSTrustStore(PKIStore.DEFAULT_CADIR, 12*3600*1000);
        ((BasicVOMSTrustStore)acTrustStore).stopRefresh();
        return acTrustStore;
    }
}
