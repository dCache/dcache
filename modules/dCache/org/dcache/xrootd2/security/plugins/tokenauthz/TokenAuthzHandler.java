package org.dcache.xrootd2.security.plugins.tokenauthz;

import java.security.GeneralSecurityException;

import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.net.InetSocketAddress;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.protocol.XrootdProtocol.FilePerm;
import org.dcache.xrootd2.security.AuthorizationHandler;
import org.dcache.xrootd2.security.plugins.tokenauthz.Envelope.GridFile;

public class TokenAuthzHandler implements AuthorizationHandler
{
    private final Map keystore;
    private String pfn = null;
    // the envelope will be initialised during checkAuthz()
    private Envelope env;
    private String noStrongAuthz;

    public TokenAuthzHandler(String noStrongAuthz, Map keystore)
    {
        this.keystore = keystore;
        this.noStrongAuthz = noStrongAuthz;
    }

    public boolean checkAuthz(String pathToOpen, Map options,
                              XrootdProtocol.FilePerm mode,
                              InetSocketAddress endpoint)
        throws GeneralSecurityException
    {
        if (pathToOpen == null) {
            throw new IllegalArgumentException("the lfn string must not be null");
        }

        String authzTokenString;
        if ((authzTokenString = (String) options.get("authz")) == null) {

            // dirty hack for ALICE: skip authorization if no token
            // arrives and configuration says ok (this will be soon
            // deprecated)
            if ("always".equalsIgnoreCase(noStrongAuthz)) {
                setPfn(pathToOpen);
                return true;
            }

            if ("read".equalsIgnoreCase(noStrongAuthz) &&
                mode == FilePerm.READ) {
                setPfn(pathToOpen);
                return true;
            }

            if ("write".equalsIgnoreCase(noStrongAuthz) &&
                mode == FilePerm.WRITE) {
                setPfn(pathToOpen);
                return true;
            }

            throw new GeneralSecurityException("No authorization token found in open request, access denied.");
        }

        // get the VO-specific keypair or the default keypair if VO
        // was not specified
        KeyPair keypair = getKeys((String) options.get("vo"));



        // decode the envelope from the token using the keypair
        // (Remote publicm key, local private key)
        Envelope env = null;
        try {
            env = decodeEnvelope(authzTokenString, keypair);
        } catch (CorruptedEnvelopeException e) {
            throw new GeneralSecurityException("Error parsing authorization token: "+e.getMessage());
        }

        this.env = env;

        // loop through all files contained in the envelope and find
        // the one with the matching lfn if no match is found, the
        // token/envelope is possibly hijacked
        GridFile file = findFile(pathToOpen, env);
        if (file == null) {
            throw new GeneralSecurityException("authorization token doesn't contain any file permissions for lfn "+pathToOpen);
        }

        // check for hostname:port in the TURL. Must match the current
        // xrootd service endpoint.  If this check fails, the token is
        // possibly hijacked
        if (!Arrays.equals(file.getTurlHost().getAddress(),
                           endpoint.getAddress().getAddress())) {
            throw new GeneralSecurityException("Hostname mismatch in authorization token (lfn="+file.getLfn()+" TURL="+file.getTurl()+")");
        }

        int turlPort =
            (file.getTurlPort() == -1)
            ? XrootdProtocol.DEFAULT_PORT
            : file.getTurlPort();
        if (turlPort != endpoint.getPort()) {
            throw new GeneralSecurityException("Port mismatch in authorization token (lfn="+file.getLfn()+" TURL="+file.getTurl()+")");
        }


        // the authorization check. read access (lowest permission
        // required) is granted by default (file.getAccess() == 0), we
        // must check only in case of writing
        int grantedPermission = file.getAccess();
        if (mode == FilePerm.WRITE) {
            if (grantedPermission < FilePerm.WRITE_ONCE.ordinal()) {
                return false;
            }
        } else if (mode == FilePerm.DELETE) {
            if (grantedPermission < FilePerm.DELETE.ordinal()) {
                return false;
            }
        }

        setPfn(file.getTurlPath());
        return true;
    }

    private GridFile findFile(String pathToOpen, Envelope env)
    {
        Iterator files = env.getFiles();
        GridFile file  = null;

        // loop through all files contained in the envelope, selecting
        // the one which maches the LFN
        while (files.hasNext()) {
            file = (GridFile) files.next();

            if (pathToOpen.equals(file.getLfn())) {
                break;
            } else {
                file = null;
            }
        }

        return file;
    }

    private Envelope decodeEnvelope(String authzTokenString, KeyPair keypair)
        throws GeneralSecurityException, CorruptedEnvelopeException
    {
        EncryptedAuthzToken token =
            new EncryptedAuthzToken(authzTokenString,
                                    (RSAPrivateKey) keypair.getPrivate(),
                                    (RSAPublicKey) keypair.getPublic());
        token.decrypt();
        return token.getEnvelope();
    }

    public boolean providesPFN()
    {
        return true;
    }

    public String getPFN()
    {
        return pfn;
    }

    private void setPfn(String pfn)
    {
        this.pfn = pfn;
    }

    private KeyPair getKeys(String vo) throws GeneralSecurityException
    {
        if (keystore == null) {
            throw new GeneralSecurityException("no keystore found");
        }

        KeyPair keypair = null;


        if (vo != null) {
            if (keystore.containsKey(vo)) {
                keypair = (KeyPair) keystore.get(vo);
            } else {
                throw new GeneralSecurityException("no keypair for VO "+vo+" found in keystore");
            }
        } else {
            // fall back to default keypair in case the VO is
            // unspecified
            if (keystore.containsKey("*")) {
                keypair = (KeyPair) keystore.get("*");
            } else {
                throw new GeneralSecurityException("no default keypair found in keystore, required for decoding authorization token");
            }
        }

        return keypair;

    }

    /**
     * Returns the FQDN of the token creator
     *
     */
    public String getUser()
    {
        return env == null ? null : env.getCreator();
    }
}
