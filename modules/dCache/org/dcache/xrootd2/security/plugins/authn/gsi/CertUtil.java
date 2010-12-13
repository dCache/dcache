package org.dcache.xrootd2.security.plugins.authn.gsi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.x500.X500Principal;

import diskCacheV111.util.Base64;

/**
 *
 * CertUtil - convenience methods for certificate processing
 *
 * @author radicke
 * @author tzangerl
 *
 */
public class CertUtil {

    private static Map<X500Principal, String> _hashCache =
        new ConcurrentHashMap<X500Principal, String>();
   /**
    * Decodes PEM by removing the given header and footer, and decodes the
    * inner content with base64.
    * @param pem the full PEM-encoded data including header + footer
    * @param header the header to be striped off
    * @param footer the footer to be striped off
    * @return the content in DER format
    */
   public static byte[] fromPEM(String pem, String header, String footer)
   {
       if (!pem.startsWith(header)) {
           throw new IllegalArgumentException(
                   "The provided PEM string doesn't start with '" + header
                           + "'");
       }

       // strip header
       StringBuilder sb = new StringBuilder(pem);
       sb.delete(0, header.length());

       removeChar(sb, '\n');

       // remove footer
       if (!sb.subSequence(sb.length() - footer.length(), sb.length()).equals(
               footer)) {
           throw new IllegalArgumentException(
                   "The provided PEM string doesn't end with '" + footer + "'");
       }
       sb.delete(sb.indexOf(footer), sb.length());

       // finally decode base64
       return Base64.base64ToByteArray(sb.toString());
   }

   /**
    * Encodes to PEM format with default X.509 certificate header/footer
    * @param der the content to be encoded
    * @return the PEM-encoded String
    */
   public static String certToPEM(byte [] der) {
       return toPEM(der,
                    "-----BEGIN CERTIFICATE-----",
                    "-----END CERTIFICATE-----");
   }

   /**
    * Encodes to PEM. The content is base64-encoded and the header and footer
    * is added.
    * @param der the content to be encoded
    * @param header the header line
    * @param footer the footer line
    * @return the PEM-encoded String
    */
   public static String toPEM(byte[] der, String header, String footer)
   {
       StringBuilder result = new StringBuilder(header);

       // make sure the header line ends with a new line char
       if (header.charAt(header.length() - 1) != '\n') {
           result.append('\n');
       }

       String base64 = Base64.byteArrayToBase64(der);

       //
       // PEM requires that each line of the BASE64-encoded data is not longer
       // than 64 characters. Therefore we insert a new line character
       // each 64 characters.
       //
       int pos = 0;
       while (pos + 64 < base64.length()) {

           result.append(base64.substring(pos, pos + 64));
           result.append('\n');

           pos += 64;
       }
       result.append(base64.substring(pos));
       result.append('\n');

       result.append(footer);

       // make sure the header line ends with a new line char
       if (footer.charAt(footer.length() - 1) != '\n') {
           result.append('\n');
       }

       return result.toString();
   }


   /**
    * Convenience method to compute a openssl-compatible md5 hash
    * @param principal the principal (either issuer or subject)
    * @return the 8-digit hexadecimal hash string
    */
   public static String computeMD5Hash(X500Principal principal)
   {
       MessageDigest md = null;
       try {
           md = MessageDigest.getInstance("MD5");
       } catch (NoSuchAlgorithmException e) {
           throw new IllegalStateException(e);
       }

       return computeHash(md, principal);
   }


   /**
    * Computes the hash from the principal, using the passed-in digest (usually MD5).
    * After applying the digest on the DER-encoded principal, the first 4 bytes of
    * the computed hash are taken and interpreted as a hexadecimal integer in Little
    * Endian. This corresponds to the openssl hash mechanism.
    *
    * Keep a cache of principals, as this method will often be called with the
    * same principal (to avoid costly rehashing).
    *
    * @param md the digest instance
    * @param principal the principal (subject or issuer)
    * @return the 8-digit hexadecimal hash
    */
   public static String computeHash(MessageDigest md, X500Principal principal)
   {
       String principalHash;

       if (_hashCache.containsKey(principal)) {
           principalHash = _hashCache.get(principal);
       } else {
           md.reset();
           md.update(principal.getEncoded());
           byte[] md5hash = md.digest();


           // take the first 4 bytes in little Endian
           int shortHash =   (0xff & md5hash[3]) << 24
                           | (0xff & md5hash[2]) << 16
                           | (0xff & md5hash[1]) << 8
                           | (0xff & md5hash[0]);

           // convert to hex
           principalHash = Integer.toHexString(shortHash);
           _hashCache.put(principal, principalHash);
       }

       return principalHash;
   }

   /**
    * remove all occurences of a character from a string
    *
    * @param sb the stringbuilder
    * @param c the char to be removed
    * @return the resulting stringbuilder
    */
   private static StringBuilder removeChar(StringBuilder sb, char c)
   {

       int index;
       while ((index = sb.indexOf("\n")) > -1) {
           sb.deleteCharAt(index);
       }

       return sb;
   }

   /**
    * Parses a sequence of certificates from an input source and returns it
    * as a list. The cert list usually represents a 'certificate path', used
    * to validate the chain of trust.
    *
    * @param in the input source
    * @return a list of x509 certificates
    * @throws IOException if an parse error occurs
    * @throws GeneralSecurityException if not certificates are found
    */
   public static List<X509Certificate> parseCerts(Reader in)
           throws IOException, GeneralSecurityException
   {

       if (in == null) {
           throw new IllegalArgumentException("no inputstream given");
       }

       List<X509Certificate> list = new LinkedList<X509Certificate>();
       X509Certificate cert = null;
       BufferedReader reader = new BufferedReader(in);
       try {
           while ((cert = org.globus.gsi.CertUtil.readCertificate(reader)) != null) {
               list.add(cert);
           }
       } finally {
           reader.close();
       }

       if (list.isEmpty()) {
           throw new GeneralSecurityException("no certificates found");
       }

       return list;
   }
}
