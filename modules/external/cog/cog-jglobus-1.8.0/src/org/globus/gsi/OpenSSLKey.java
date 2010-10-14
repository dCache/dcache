/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi;

import java.util.StringTokenizer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.Reader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.Writer;
import java.security.SecureRandom;
import java.security.MessageDigest;
import java.security.InvalidKeyException;
import java.security.PrivateKey;
import java.security.Key;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

import org.globus.util.Base64;
import org.globus.util.PEMUtils;
import org.globus.util.Util;
import org.globus.util.I18n;

/**
 * Represents a OpenSSL-style PEM-formatted private key. It supports encryption
 * and decryption of the key. Currently, only RSA keys are supported, 
 * and only TripleDES encryption is supported.
 * This is based on work done by Ming Yung at DSTC.
 */
public abstract class OpenSSLKey {

    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     OpenSSLKey.class.getClassLoader());

    public static final String HEADER = "-----BEGIN RSA PRIVATE KEY-----";
    
    /* Key algorithm: RSA, DSA */
    private String keyAlg       = null;
    /* Current state of this key class */
    private boolean isEncrypted = false;

    // base64 encoded key value
    private byte[] encodedKey   = null; 
    private PrivateKey intKey   = null;
    private IvParameterSpec iv  = null;

    /*
     * String representation of the encryption algorithm:
     * DES-EDE3-CBC, AES-256-CBC, etc. 
     */
    private String encAlgStr    = null;
    
    /*
     * Java string representation of the encryption algorithm:
     * DES, DESede, AES. 
     */
    private String encAlg       = null;
    private int keyLength       = -1;
    private int ivLength        = -1;

    // ASN.1 encoded key value
    private byte[] keyData      = null;
    
    /**
     * Reads a OpenSSL private key from the specified input stream.
     * The private key must be PEM encoded and can be encrypted.
     *
     * @param is input stream with OpenSSL key in PEM format.
     * @exception IOException if I/O problems.
     * @exception GeneralSecurityException if problems with the key
     */
    public OpenSSLKey(InputStream is) 
        throws IOException,  GeneralSecurityException {
        InputStreamReader isr = new InputStreamReader(is);
        readPEM(isr);
    }

    /**
     * Reads a OpenSSL private key from the specified file.
     * The private key must be PEM encoded and can be encrypted.
     *
     * @param file file containing the OpenSSL key in PEM format.
     * @exception IOException if I/O problems.
     * @exception GeneralSecurityException if problems with the key
     */
    public OpenSSLKey(String file)
        throws IOException, GeneralSecurityException {
        FileReader f = null;
        try {
            f = new FileReader(file);
            readPEM(f);
        } finally {
            if (f != null) f.close();
        }
    }
    
    /**
     * Converts a RSAPrivateCrtKey into OpenSSL key.
     *
     * @param key private key - must be a RSAPrivateCrtKey
     */
    public OpenSSLKey(PrivateKey key) {
        this.intKey = key;
        this.isEncrypted = false;
        this.keyData = getEncoded(key);
        this.encodedKey = null;
    }

    /**
     * Initializes the OpenSSL key from raw byte array.
     *
     * @param algorithm  the algorithm of the key. Currently
     *                   only RSA algorithm is supported.
     * @param data       the DER encoded key data. If RSA
     *                   algorithm, the key must be in
     *                   PKCS#1 format.
     * @exception GeneralSecurityException if any security 
     *            problems.
     */
    public OpenSSLKey(String algorithm, byte [] data) 
        throws GeneralSecurityException {
        if (data == null) {
            throw new IllegalArgumentException(i18n.getMessage("dataNull"));
        }
        this.keyData = data;
        this.isEncrypted = false;
        this.intKey = getKey(algorithm, data);
    }
  
    protected byte[] getEncoded() {
        return this.keyData;
    }

    private void readPEM(Reader rd) 
        throws IOException, GeneralSecurityException {

        BufferedReader in = new BufferedReader(rd);
        
        StringBuffer sb = new StringBuffer();
        
        String next = null;
        
        while( (next = in.readLine()) != null) {
            if (next.indexOf("PRIVATE KEY") != -1) {
                keyAlg = getKeyAlgorithm(next);
                break;
            }
        }
         
        if (next == null) {
            throw new InvalidKeyException(i18n.getMessage("noPrivateKey"));
        }

        if (keyAlg == null) {
            throw new InvalidKeyException(i18n.getMessage("algNotSup"));
        }
        
        next = in.readLine();
        if (next.startsWith("Proc-Type: 4,ENCRYPTED")) {
            this.isEncrypted = true;
            parseEncryptionInfo(in.readLine());
            in.readLine();
        } else {
            this.isEncrypted = false;
            sb.append(next);
        }
        
        while ( (next = in.readLine()) != null ) {
            if (next.startsWith("-----END")) break;
            sb.append(next); 
        }
         
        this.encodedKey = sb.toString().getBytes();
        
        if (!isEncrypted()) {
            this.keyData = Base64.decode( encodedKey );
            this.intKey  = getKey(keyAlg, keyData);
        } else {
            this.keyData = null;
        }
    }
  
    /**
     * Check if the key was encrypted or not.
     *
     * @return true if the key is encrypted, false
     *         otherwise.
     */
    public boolean isEncrypted() {
        return this.isEncrypted;
    }

    /**
     * Decrypts the private key with given password.
     * Does nothing if the key is not encrypted.
     *
     * @param password password to decrypt the key with.
     * @exception GeneralSecurityException
     *            whenever an error occurs during decryption.
     * @exception InvalidKeyException
     *            whenever an error occurs during decryption.
     */
    public void decrypt(String password) 
        throws GeneralSecurityException, InvalidKeyException {
        decrypt(password.getBytes());
    }
    
    /**
     * Decrypts the private key with given password.
     * Does nothing if the key is not encrypted.
     *
     * @param password password to decrypt the key with.
     * @exception GeneralSecurityException
     *            whenever an error occurs during decryption.
     * @exception InvalidKeyException
     *            whenever an error occurs during decryption.
     */
    public void decrypt(byte [] password) 
        throws GeneralSecurityException, InvalidKeyException {
        if (!isEncrypted()) {
            return;
        }
        
        byte [] enc = Base64.decode(this.encodedKey);
        
        SecretKeySpec key = getSecretKey(password, this.iv.getIV());
        
        Cipher cipher = getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, this.iv);
        enc = cipher.doFinal(enc);
        
        this.intKey = getKey(this.keyAlg, enc);
        this.keyData = enc;
        this.isEncrypted = false;
        this.encodedKey = null;
    }
    
    /**
     * Encrypts the private key with given password.
     * Does nothing if the key is encrypted already.
     *
     * @param password password to encrypt the key with.
     * @exception GeneralSecurityException
     *            whenever an error occurs during encryption.
     */
    public void encrypt(String password) 
        throws GeneralSecurityException {
        encrypt(password.getBytes());
    }
  
    /**
     * Encrypts the private key with given password.
     * Does nothing if the key is encrypted already.
     *
     * @param password password to encrypt the key with.
     * @exception GeneralSecurityException
     *            whenever an error occurs during encryption.
     */
    public void encrypt(byte [] password) 
        throws GeneralSecurityException {
        
        if (isEncrypted()) {
            return;
        }
        
        if (this.encAlg == null) {
            setEncryptionAlgorithm("DES-EDE3-CBC");
        }

        if (this.iv == null) {
            this.iv = generateIV();
        }
         
        Key key = getSecretKey(password, this.iv.getIV());
         
        Cipher cipher = getCipher();
        cipher.init(Cipher.ENCRYPT_MODE, key, this.iv);
         
        /* encrypt the raw PKCS11 */
        
        this.keyData = cipher.doFinal( getEncoded(this.intKey) );
        this.isEncrypted = true;
        this.encodedKey = null;
    }
    
    /**
     * Sets algorithm for encryption.
     *
     * @param alg algorithm for encryption
     * @throws GeneralSecurityException if algorithm is not supported
     */
    public void setEncryptionAlgorithm(String alg) 
        throws GeneralSecurityException {
        setAlgorithmSettings(alg);
    }
    
    /**
     * Returns the JCE (RSAPrivateCrtKey) key.
     *
     * @return the private key, null if the key
     *         was not decrypted yet.
     */
    public PrivateKey getPrivateKey() {
        return this.intKey;
    }
  
    /**
     * Writes the private key to the specified output stream in PEM
     * format. If the key was encrypted it will be encoded as an encrypted
     * RSA key. If not, it will be encoded as a regular RSA key.
     *
     * @param output output stream to write the key to.
     * @exception IOException if I/O problems writing the key
     */
    public void writeTo(OutputStream output) 
        throws IOException {
        output.write( toPEM().getBytes() );
    }
  
    /**
     * Writes the private key to the specified writer in PEM format. 
     * If the key was encrypted it will be encoded as an encrypted
     * RSA key. If not, it will be encoded as a regular RSA key.
     *
     * @param w writer to output the key to.
     * @exception IOException if I/O problems writing the key
     */
    public void writeTo(Writer w) 
        throws IOException {
        w.write( toPEM() );
    }
    
    /**
     * Writes the private key to the specified file in PEM format.
     * If the key was encrypted it will be encoded as an encrypted
     * RSA key. If not, it will be encoded as a regular RSA key.
     *
     * @param file file to write the key to.
     * @exception IOException if I/O problems writing the key
     */
    public void writeTo(String file) 
        throws IOException {
        PrintWriter p = null;
        try {
            File f = Util.createFile(file);
            Util.setOwnerAccessOnly(file);
            p = new PrintWriter(new FileOutputStream(f));
            p.write( toPEM() );
        } finally {
            if (p != null) p.close();
        }
    }
    
    /**
     * Returns DER encoded byte array (PKCS#1). 
     */
    protected abstract byte[] getEncoded(PrivateKey key);
    
    /**
     * Returns PrivateKey object initialized from give byte array 
     * (in PKCS#1 format)
     */
    protected abstract PrivateKey getKey(String alg, byte [] data) 
        throws GeneralSecurityException;
    
    protected String getProvider() {
        return null;
    }
    
    private Cipher getCipher() 
        throws GeneralSecurityException {
        String provider = getProvider();
        if (provider == null) {
            return Cipher.getInstance(this.encAlg + "/CBC/PKCS5Padding");
        } else {
            return Cipher.getInstance(this.encAlg + "/CBC/PKCS5Padding",
                                      provider);
        }
    }

    private String getKeyAlgorithm(String line) {
        if (line.indexOf("RSA") != -1) {
            return "RSA";
        } else if (line.indexOf("DSA") != -1) {
            return "DSA";
        } else {
            return null;
        }
    }
    
    private void parseEncryptionInfo(String line) 
        throws GeneralSecurityException {
        // TODO: can make this better
        String keyInfo = line.substring(10);
        StringTokenizer tknz = new StringTokenizer(keyInfo, ",", false);
        // set algorithm settings
        setAlgorithmSettings(tknz.nextToken());
        // set IV
        setIV(tknz.nextToken());
    }
  
    private void setAlgorithmSettings(String alg) 
        throws GeneralSecurityException {
        if (alg.equals("DES-EDE3-CBC")) {
            this.encAlg = "DESede";
            this.keyLength = 24;
            this.ivLength = 8;
        } else if (alg.equals("AES-128-CBC")) {
            this.encAlg = "AES";
            this.keyLength = 16;
            this.ivLength = 16;
        } else if (alg.equals("AES-192-CBC")) {
            this.encAlg = "AES";
            this.keyLength = 24;
            this.ivLength = 16;
        } else if (alg.equals("AES-256-CBC")) {
            this.encAlg = "AES";
            this.keyLength = 32;
            this.ivLength = 16;
        } else if (alg.equals("DES-CBC")) {
            this.encAlg = "DES";
            this.keyLength = 8;
            this.ivLength = 8;
        } else {
            throw new GeneralSecurityException(i18n.getMessage("unsupEnc",
                                                               alg));
        }
        this.encAlgStr = alg;
    }

    private void setIV(String s) 
        throws GeneralSecurityException {
        int len = s.length() / 2;
        if (len != this.ivLength) {
            String err = i18n.getMessage("ivLength", new String[] { 
                Integer.toString(this.ivLength), Integer.toString(len) });
            throw new GeneralSecurityException(err);
        }
        byte[] ivBytes = new byte[len];
        for (int j=0; j<len; j++) {
            ivBytes[j] = (byte)Integer.parseInt(s.substring(j*2, j*2 + 2), 16);
        }        
        this.iv = new IvParameterSpec(ivBytes);
    }
    
    private IvParameterSpec generateIV() {
        byte [] b = new byte[this.ivLength];
        SecureRandom sr = new SecureRandom(); //.getInstance("PRNG");
        sr.nextBytes(b);
        return new IvParameterSpec(b);
    }

    private SecretKeySpec getSecretKey(byte [] pwd, byte[] iv)
        throws GeneralSecurityException {

        byte[] key = new byte[this.keyLength];
        int offset = 0;
        int bytesNeeded = this.keyLength;

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        for (;;) {
            md5.update(pwd);
            md5.update(iv, 0, 8);

            byte[] b = md5.digest();

            int len = (bytesNeeded > b.length) ? b.length : bytesNeeded;

            System.arraycopy(b, 0, key, offset, len);

            offset += len;

            // check if we need any more
            bytesNeeded = key.length - offset;
            if (bytesNeeded == 0) {
                break;
            }

            // do another round
            md5.reset();
            md5.update(b);
        }
        
        return new SecretKeySpec(key, this.encAlg);
    }

    // -------------------------------------------
    
    /**
     * Converts to PEM encoding.
     * Assumes keyData is initialized.
     */
    private String toPEM() {
        
        byte [] data = (this.keyData == null) ? 
            this.encodedKey : Base64.encode(this.keyData);
        
        String header = HEADER;

        if (isEncrypted()) {
            StringBuffer buf = new StringBuffer(header);
            buf.append(PEMUtils.lineSep);
            buf.append("Proc-Type: 4,ENCRYPTED");
            buf.append(PEMUtils.lineSep);
            buf.append("DEK-Info: ").append(this.encAlgStr);
            buf.append(",").append(PEMUtils.toHex(iv.getIV()));
            buf.append(PEMUtils.lineSep);
            header = buf.toString();
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            PEMUtils.writeBase64(out,
                                 header,
                                 data,
                                 "-----END RSA PRIVATE KEY-----");
        } catch (IOException e) {
            throw new RuntimeException(i18n.getMessage("unexpErr") + 
                                       e.getMessage());
        }
        
        return new String(out.toByteArray());
    }
}
