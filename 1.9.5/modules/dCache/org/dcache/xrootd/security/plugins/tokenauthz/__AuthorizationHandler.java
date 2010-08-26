package org.dcache.xrootd.security.plugins.tokenauthz;

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
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.dcache.doors.XrootdDoor;
import org.dcache.xrootd.security.plugins.tokenauthz.Envelope.GridFile;
import org.dcache.xrootd.util.ParseException;


public class __AuthorizationHandler {


    private static Map keystore = null;

    //	the RSA keyfactory
    static private KeyFactory keyFactory;

    private XrootdDoor door;



    public __AuthorizationHandler(XrootdDoor door) {
        this.door = door;
    }

    public  GridFile checkAuthz(String tokenString, String vo, String lfn_Path) throws GeneralSecurityException, CorruptedEnvelopeException {

        if (tokenString == null) {
            throw new IllegalArgumentException("the token string must not be null");
        }
        if (lfn_Path == null) {
            throw new IllegalArgumentException("the lfn string must not be null");
        }

        KeyPair keypair = null;

        if (vo != null) {
            if (keystore.containsKey(vo)) {
                keypair = (KeyPair) keystore.get(vo);
            } else {
                throw new GeneralSecurityException("no keypair for VO "+vo+" found in keystore");
            }
        } else {
            //			fall back to default keypair in case the VO is unspecified
            if (keystore.containsKey("*")) {
                keypair = (KeyPair) keystore.get("*");
                door.say((vo != null ? "keypair for VO "+vo+" not found in keystore" : "no VO scecified in open request") + ", using default keypair");
            } else {
                throw new GeneralSecurityException("no default keypair found in keystore");
            }
        }



        EncryptedAuthzToken token = new EncryptedAuthzToken(
                                                            tokenString,
                                                            (RSAPrivateKey) keypair.getPrivate(),
                                                            (RSAPublicKey) keypair.getPublic());

        token.decrypt();
        Envelope env = token.getEnvelope();

        Iterator files = env.getFiles();
        GridFile file  = null;

        //		loop through all specified files and find the one with the matching lfn
        //		if no match is found, the token is possibly hijacked
        while (files.hasNext()) {
            file = (GridFile) files.next();

            if (lfn_Path.equals(file.getLfn())) {
                break;
            } else {
                file = null;
            }
        }

        if (file == null) {
            throw new GeneralSecurityException("authorization token doesn't contain any file permissions for lfn "+lfn_Path);
        }

        //		check for certain protocol
        //		if (!file.getTurlProtocol().equals("root")) {
        //			throw new GeneralSecurityException("wrong protocol in turl. must be 'root'");
        //		}


        //		check for hostname:port in the TURL. Must match the current xrootd service endpoint.
        //		If this check fails, the token is possibly hijacked
        if ( !( file.getTurlHost().equals(door.getDoorHost()) && file.getTurlPort() == door.getDoorPort())) {
            throw new GeneralSecurityException("Hostname and/or Port mismatch in authorization token (lfn="+file.getLfn()+" TURL="+file.getTurl()+")");
        }

        return file;
    }


    public static void loadKeyStore(String fileName) throws ParseException,  IOException {

        if (keystore != null) {
            return;
        }

        File keystoreFile = new File(fileName);

        LineNumberReader in = new LineNumberReader(new FileReader(keystoreFile));

        //		reset keystore
        keystore = new Hashtable();

        try {
            //			initialise RSA key factory
            keyFactory = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e1) {}

        String line = null;
        while ((line = in.readLine()) != null) {

            StringTokenizer tokenizer = new StringTokenizer(line, " \t");

            String voToken = null;
            String privKeyToken = null;
            String pubKeyToken = null;

            try {

                //				ignore comment lines and any lines not starting with the keyword 'KEY'
                String firstToken = tokenizer.nextToken();
                if (firstToken.startsWith("#") || !firstToken.equals("KEY")) {
                    continue;
                }

                voToken = tokenizer.nextToken();
                privKeyToken = tokenizer.nextToken();
                pubKeyToken = tokenizer.nextToken();

            } catch (NoSuchElementException e) {
                throw new ParseException("line no "+(in.getLineNumber())+" : invalid format");
            }

            if (	!(	voToken.startsWith("VO:") &&
                                privKeyToken.startsWith("PRIVKEY:") &&
                                pubKeyToken.startsWith("PUBKEY:"))) {

                throw new ParseException("line no "+(in.getLineNumber())+" : invalid format");
            }


            keystore.put(
                         voToken.substring(voToken.indexOf(':') + 1),
                         loadKeyPair(privKeyToken.substring(privKeyToken.indexOf(':') + 1),
                                     pubKeyToken.substring(pubKeyToken.indexOf(':') + 1)));

        }


    }

    private static KeyPair loadKeyPair(String privKeyFileName, String pubKeyFileName) throws IOException {

        File privKeyFile = new File(privKeyFileName);
        File pubKeyFile = new File(pubKeyFileName);

        byte[] privKeyArray = readKeyfile(privKeyFile);
        //		logger.debug("read private keyfile "+privKeyFile+" ("+privKeyArray.length+" bytes)");
        //		store private key (DER-encoded) in PKCS8-representation object
        PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privKeyArray);
        //		parse unencrypted private key into java private key object
        RSAPrivateKey privKey;
        try {
            privKey = (RSAPrivateKey) keyFactory.generatePrivate(privKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new IOException("error loading private key "+privKeyFileName+": "+e.getMessage());
        }

        byte[] pubKeyArray = readKeyfile(pubKeyFile);
        //		logger.debug("Read public keyfile "+pubKeyFile+" ("+pubKeyArray.length+" bytes)");
        //		store the public key (DER-encodedn ot PEM) into a X.509 certificate object
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyArray);
        RSAPublicKey pubKey;
        try {
            pubKey = (RSAPublicKey) keyFactory.generatePublic(pubKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new IOException("error loading public key "+pubKeyFileName+": "+e.getMessage());
        }

        return new KeyPair(pubKey, privKey);
    }


    /**
     * Helper method thats reads a file.
     * @param file the File which is going to be read
     * @return an array which holds the file content
     * @throws IOException if reading the file fails
     */
    private static byte[] readKeyfile(File file) throws IOException {

        InputStream in = new FileInputStream(file);

        byte[] result = new byte[(int) file.length()];
        int bytesRead = 0;

        while ((bytesRead += in.read(result,bytesRead,(int) file.length()-bytesRead)) < file.length());

        if (bytesRead != file.length()) {
            throw new IOException("Keyfile "+file.getName()+" corrupt.");
        }

        in.close();

        return result;

    }

}
