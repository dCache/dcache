package org.dcache.xrootd2.security.plugins.tokenauthz;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Hashtable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.dcache.xrootd2.security.AbstractAuthorizationFactory;
import org.dcache.xrootd2.security.AuthorizationHandler;
import org.dcache.xrootd2.util.ParseException;

public class TokenAuthorizationFactory implements AbstractAuthorizationFactory
{
    private Map keystore;
    private File keystoreFile;

    /**
     * Dirty hack: will be deprecated soon
     */
    private String _noStrongAuthz;

    public AuthorizationHandler getAuthzHandler()
    {
        return new TokenAuthzHandler(_noStrongAuthz, keystore);
    }

    private void loadKeyStore() throws ParseException,  IOException
    {
        LineNumberReader in =
            new LineNumberReader(new FileReader(keystoreFile));
        try {
            // reset keystore
            keystore = new Hashtable();

            // the RSA keyfactory
            KeyFactory keyFactory = null;

            try {
                // initialise RSA key factory
                keyFactory = KeyFactory.getInstance("RSA");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Failed to initialize RSA key factory" +
                                           e.getMessage());
            }

            String line = null;
            while ((line = in.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, " \t");

                String voToken = null;
                String privKeyToken = null;
                String pubKeyToken = null;

                try {

                    // ignore comment lines and any lines not starting
                    // with the keyword 'KEY'
                    String firstToken = tokenizer.nextToken();
                    if (firstToken.startsWith("#") || !firstToken.equals("KEY")) {
                        continue;
                    }

                    voToken = tokenizer.nextToken();
                    privKeyToken = tokenizer.nextToken();
                    pubKeyToken = tokenizer.nextToken();

                } catch (NoSuchElementException e) {
                    throw new ParseException("line no " + (in.getLineNumber()) +
                                             " : invalid format");
                }

                if (!(voToken.startsWith("VO:") &&
                      privKeyToken.startsWith("PRIVKEY:") &&
                      pubKeyToken.startsWith("PUBKEY:"))) {
                    throw new ParseException("line no " + (in.getLineNumber()) +
                                             " : invalid format");
                }


                keystore.put(voToken.substring(voToken.indexOf(':') + 1),
                             loadKeyPair(privKeyToken.substring(privKeyToken.indexOf(':') + 1),
                                         pubKeyToken.substring(pubKeyToken.indexOf(':') + 1), keyFactory));
            }
        } finally {
            in.close();
        }
    }

    private KeyPair loadKeyPair(String privKeyFileName, String pubKeyFileName,
                                KeyFactory keyFactory)
        throws IOException
    {
        File privKeyFile = new File(privKeyFileName);
        File pubKeyFile = new File(pubKeyFileName);

        byte[] privKeyArray = readKeyfile(privKeyFile);
        // logger.debug("read private keyfile "+privKeyFile+" ("+privKeyArray.length+" bytes)");
        // store private key (DER-encoded) in PKCS8-representation object
        PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privKeyArray);
        // parse unencrypted private key into java private key object
        RSAPrivateKey privKey;
        try {
            privKey = (RSAPrivateKey) keyFactory.generatePrivate(privKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new IOException("error loading private key "+privKeyFileName+": "+e.getMessage());
        }

        byte[] pubKeyArray = readKeyfile(pubKeyFile);
        // logger.debug("Read public keyfile "+pubKeyFile+" ("+pubKeyArray.length+" bytes)");
        // store the public key (DER-encodedn ot PEM) into a X.509 certificate object
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyArray);
        RSAPublicKey pubKey;
        try {
            pubKey = (RSAPublicKey) keyFactory.generatePublic(pubKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new IOException("error loading public key " +
                                  pubKeyFileName + ": " + e.getMessage());
        }

        return new KeyPair(pubKey, privKey);
    }


    /**
     * Helper method thats reads a file.
     * @param file the File which is going to be read
     * @return an array which holds the file content
     * @throws IOException if reading the file fails
     */
    private static byte[] readKeyfile(File file) throws IOException
    {
        InputStream in = new FileInputStream(file);

        byte[] result = new byte[(int) file.length()];
        int bytesRead = 0;

        while ((bytesRead += in.read(result, bytesRead, (int) file.length()-bytesRead)) < file.length());

        if (bytesRead != file.length()) {
            throw new IOException("Keyfile "+file.getName()+" corrupt.");
        }

        in.close();

        return result;
    }

    public void init() throws GeneralSecurityException
    {
        try {
            loadKeyStore();
        } catch (Exception e) {
            throw new GeneralSecurityException("unable to load keystore: "+e.getMessage());
        }
    }

    public void setKeystore(String file)
    {
        keystoreFile = new File(file);
    }

    public String getKeystore()
    {
        return keystoreFile.toString();
    }

    public void setNoStrongAuthorization(String auth)
    {
        _noStrongAuthz = auth;
    }

    public String getNoStrongAuthorization()
    {
        return _noStrongAuthz;
    }
}
