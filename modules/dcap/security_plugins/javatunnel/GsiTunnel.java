/*
 * $Id: GsiTunnel.java,v 1.6 2006-10-11 09:49:58 tigran Exp $
 */

package javatunnel;

import java.io.*;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.List;

//jgss
import org.ietf.jgss.*;

// globus gsi
import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.VOMSValidator;
import org.glite.security.voms.FQAN;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.TrustedCertificates;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;


class GsiTunnel extends GssTunnel  {

    private ExtendedGSSContext _e_context = null;

    MessageProp _prop =  new MessageProp(true);

    private String service_key           = "/etc/grid-security/hostkey.pem";
    private String service_cert          = "/etc/grid-security/hostcert.pem";
    private String service_trusted_certs = "/etc/grid-security/certificates";

    // Creates a new instance of GssTunnel
    public GsiTunnel(String dummy) {
        this(dummy, true);
    }


    public GsiTunnel(String dummy, boolean init) {
        if( init ) {
            try {
                GlobusCredential serviceCredential;

                serviceCredential =new GlobusCredential(service_cert, service_key);

                GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential, GSSCredential.ACCEPT_ONLY);
                TrustedCertificates trusted_certs = TrustedCertificates.load(service_trusted_certs);
                GSSManager manager = ExtendedGSSManager.getInstance();
                _e_context = (ExtendedGSSContext) manager.createContext(cred);
                _e_context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
                _e_context.setOption(GSSConstants.TRUSTED_CERTIFICATES, trusted_certs);

                _context = _e_context;
                // do not use cannel binding with GSIGSS
                super.useChannelBinding(false);

            }catch( Exception e ) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public boolean verify( InputStream in, OutputStream out, Object addon) {

        try {
        	if( super.verify(in, out, addon) ) {
        		scanExtendedAttributes(_e_context);
        	}
        } catch( Exception e) { }

        return _context.isEstablished();
    }


    @Override
    public Convertable makeCopy() {
        return new GsiTunnel( null, true  );
    }

    private void scanExtendedAttributes(ExtendedGSSContext gssContext)  {


		String fqanValue = null;
		try {

			X509Certificate[] chain = (X509Certificate[]) gssContext
					.inquireByOid(GSSConstants.X509_CERT_CHAIN);

			if(chain == null ) {
				// some thing is wrong here
				return;
			}

			VOMSValidator validator = new VOMSValidator(chain);
			validator.parse();

			List listOfAttributes = validator.getVOMSAttributes();

			Iterator i = listOfAttributes.iterator();

			// currently take first one only

			if (i.hasNext()) {

				VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
				List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
				Iterator j = listOfFqans.iterator();

				_group = vomsAttribute.getVO();
				if (j.hasNext()) {
					fqanValue = (String) j.next();
					FQAN fqan = new FQAN(fqanValue);
					_role = fqanValue;
				}
			}

		}catch(Exception e) {
			e.printStackTrace();
		}

	}

}
