package org.dcache.xrootd2.security.plugins.authn.gsi;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.security.auth.Subject;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;
import static org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.*;
import static org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType.*;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd2.protocol.messages.AuthenticationRequest;
import org.dcache.xrootd2.protocol.messages.AuthenticationResponse;
import org.dcache.xrootd2.protocol.messages.ErrorResponse;
import org.dcache.xrootd2.protocol.messages.OKResponse;
import org.dcache.xrootd2.security.AuthenticationHandler;
import org.dcache.xrootd2.security.plugins.authn.RawBucket;
import org.dcache.xrootd2.security.plugins.authn.XrootdBucket;
import org.dcache.xrootd2.security.plugins.authn.NestedBucketBuffer;
import org.dcache.xrootd2.security.plugins.authn.StringBucket;
import org.globus.gsi.CertificateRevocationLists;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.globus.gsi.proxy.ProxyPathValidator;
import org.globus.gsi.proxy.ProxyPathValidatorException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for xrootd-security message exchange based on the GSI protocol.
 * Loosely based on the first reverse-engineering of xrootdsec-gsi, done by
 * Martin Radicke.
 *
 * @author tzangerl
 *
 */
public class GSIAuthenticationHandler implements AuthenticationHandler
{
    public static final String PROTOCOL = "gsi";
    public static final String PROTOCOL_VERSION= "10200";
    public static final String CRYPTO_MODE = "ssl";
    /** for now, we limit ourselves to AES-128 with CBC blockmode. */
    public static final String SUPPORTED_CIPHER_ALGORITHMS = "aes-128-cbc";
    public static final String SUPPORTED_DIGESTS = "sha1:md5";

    private final static Logger _logger =
        LoggerFactory.getLogger(GSIAuthenticationHandler.class);

    /**
     * RSA algorithm, no block chaining mode (not a block-cipher) and PKCS1
     * padding, which is recommended to be used in conjunction with RSA
     */
    private final static String SERVER_ASYNC_CIPHER_MODE = "RSA/NONE/PKCS1Padding";

    /** the sync cipher mode supported by the server. Unless this is made
     * configurable (todo), it has to match the SUPPORTED_CIPHER_ALGORITHMS
     * advertised by the server
     */
    private final static String SERVER_SYNC_CIPHER_MODE = "AES/CBC/PKCS5Padding";
    private final static String SERVER_SYNC_CIPHER_NAME = "AES";
    /** blocksize in bytes */
    private final static int SERVER_SYNC_CIPHER_BLOCKSIZE = 16;
    private final static int CHALLENGE_BYTES = 8;

    /** cryptographic helper classes */
    private static final SecureRandom _random = new SecureRandom();
    private static final ProxyPathValidator _proxyValidator =
        new ProxyPathValidator();


    /** certificates/keys/trust-anchors */
    private TrustedCertificates _trustedCerts;
    private X509Certificate _hostCertificate;
    private PrivateKey _hostKey;
    private CertificateRevocationLists _crls;

    private String _challenge = "";
    private Cipher _challengeCipher;
    private DHSession _dhSession;

    /**
     * Container for principals and credentials found during the authentication
     * process.
     */
    private Subject _subject;

    private boolean _finished = false;

    /**
     * @param hostCertificatePath
     * @param hostKeyPath
     * @param caCertDir
     * @param verifyHostCertificate
     * @param endpoint CellEndpoint for communication with the login strategies
     */
    public GSIAuthenticationHandler(X509Certificate hostCertificate,
                                    PrivateKey privateKey,
                                    TrustedCertificates trustedCerts,
                                    CertificateRevocationLists crls) {

        _hostCertificate = hostCertificate;
        _hostKey = privateKey;
        _trustedCerts = trustedCerts;
        _crls = crls;

        _subject = new Subject();
    }

    class XrootdBucketContainer {
        private int _size;
        private List<XrootdBucket> _buckets;

        public XrootdBucketContainer(List<XrootdBucket> buckets, int size) {
            _buckets = buckets;
            _size = size;
        }

        public int getSize() {
            return _size;
        }

        public List<XrootdBucket> getBuckets() {
            return _buckets;
        }
    }

    /**
     * dispatcher function that initializes the diffie-hellman key agreement
     * session, checks the request for the correct protocol and calls the
     * actual handler functions.
     *
     * @see #handleCertReqStep
     * @see #handleCertStep
     */
    @Override
    public AbstractResponseMessage authenticate(AuthenticationRequest request) {

        try {
            if (_dhSession == null) {
                _dhSession = new DHSession();
            }
        } catch (GeneralSecurityException gssex) {
            _logger.error("Error setting up cryptographic classes: {}",
                          gssex);
            return new ErrorResponse(request.getStreamID(),
                                     kXR_ServerError,
                                     "Server probably misconfigured.");
        }

        /* check whether the protocol matches */
        if (!PROTOCOL.equalsIgnoreCase(request.getProtocol())) {
            return new ErrorResponse(request.getStreamID(),
                                     kXR_InvalidRequest,
                                     "Specified Protocol " + request.getProtocol() +
                                     " is not the protocol that was negotiated.");
        }

        switch(request.getStep()) {
        case kXGC_none:
            return new OKResponse(request.getStreamID());
        case kXGC_certreq:
            return handleCertReqStep(request);
        case kXGC_cert:
            return handleCertStep(request);
        default:
            return new ErrorResponse(request.getStreamID(),
                                     kXR_InvalidRequest,
                                     "Error during authentication, " +
                                     "unknown processing step: "
                                     + request.getStep());
        }
    }

    /**
     * Handle the kXGC_certreq step, as signalled by the client. Load host
     * credentials, decode received kXR buckets and build a response
     * consisting of reply buckets.
     *
     * The cert-req step will consist of a client challenge (rTag) that is
     * signed by the server using its private key. The public key, needed
     * by the client for verification, is sent along with the response.
     *
     * Other information passed by the server include DH-parameters needed for
     * symmetric key exchange, a list of supported symmetric ciphers and
     * digests.
     *
     * @param request The received authentication request
     * @return AuthenticationResponse with kXR_authmore or ErrorResponse if
     *         something failed
     */
    private AbstractResponseMessage handleCertReqStep(AuthenticationRequest request) {

        AbstractResponseMessage response;

        try {
            _challengeCipher = Cipher.getInstance(SERVER_ASYNC_CIPHER_MODE,
                                                  "BC");

            _challengeCipher.init(Cipher.ENCRYPT_MODE, _hostKey);

            Map<BucketType, XrootdBucket> buckets = request.getBuckets();
            NestedBucketBuffer buffer =
                ((NestedBucketBuffer) buckets.get(kXRS_main));

            StringBucket rtagBucket =
                (StringBucket) buffer.getNestedBuckets().get(kXRS_rtag);
            String rtag = rtagBucket.getContent();

            /* sign the rtag for the client */
            _challengeCipher.update(rtag.getBytes());
            byte [] signedRtag = _challengeCipher.doFinal();
            /* generate a new challenge, to be signed by the client */
            _challenge = generateChallengeString();
            /* send DH params */
            byte[] puk = _dhSession.getEncodedDHMaterial().getBytes();
            /* send host certificate */
            String hostCertificateString =
                CertUtil.certToPEM(_hostCertificate.getEncoded());

            XrootdBucketContainer responseBuckets =
                            buildCertReqResponse(signedRtag,
                                                 _challenge,
                                                 CRYPTO_MODE,
                                                 puk,
                                                 SUPPORTED_CIPHER_ALGORITHMS,
                                                 SUPPORTED_DIGESTS,
                                                 hostCertificateString);

            response =  new AuthenticationResponse(request.getStreamID(),
                                                   XrootdProtocol.kXR_authmore,
                                                   responseBuckets.getSize(),
                                                   PROTOCOL,
                                                   kXGS_cert,
                                                   responseBuckets.getBuckets());
        } catch (InvalidKeyException ikex) {
            _logger.error("Configured host-key could not be used for" +
                          "signing rtag: {}", ikex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_ServerError,
                                         "Internal error occured when trying " +
                                         "to sign client authentication tag.");
        } catch (CertificateEncodingException cee) {
            _logger.error("Could not extract contents of server certificate:" +
                          " {}", cee);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_ServerError,
                                         "Internal error occured when trying " +
                                         "to send server certificate.");
        } catch (GeneralSecurityException gssex) {
            _logger.error("Problems during signing of client authN tag " +
                          "(algorithm {}): {}", SERVER_ASYNC_CIPHER_MODE, gssex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_ServerError,
                                         "Internal error occured when trying " +
                                         "to sign client authentication tag.");
        }  catch (RuntimeException rtex) {
            _logger.error("Processing of kXRC_certreq failed due to a bug: {}",
                          rtex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_ServerError,
                                         "An error occured while processing " +
                                         "the cert request.");
        }

        return response;
    }

    /**
     * Handle the second step (reply by client to authmore).
     * In this step the DH-symmetric key agreement is finalized, thus obtaining
     * a symmetric key that can subsequently be used to encrypt the message
     * exchange.
     *
     * The challenge cipher sent by the server in the kXR_cert step is sent
     * back. The cipher is signed by the client's private key, we can use the
     * included public key to verify it.
     *
     * Also, the client's X.509 certificate will be checked for trustworthiness.
     * Presently, this check is limited to verifying whether the issuer
     * certificate is trusted and the certificate is not contained in a CRL
     * installed on the server.
     *
     * @param request AuthenticationRequest received by the client
     * @return OKResponse (verification is okay) or ErrorResponse if something
     *         went wrong
     */
    private AbstractResponseMessage handleCertStep(AuthenticationRequest request) {

        AbstractResponseMessage response;

        try {
            Map<BucketType, XrootdBucket> receivedBuckets = request.getBuckets();

            /* the stuff we want to get is the encrypted material in kXRS_main */
            RawBucket encryptedBucket =
                (RawBucket) receivedBuckets.get(kXRS_main);

            byte [] encrypted = encryptedBucket.getContent();

            StringBucket dhMessage =
                (StringBucket) receivedBuckets.get(kXRS_puk);

            _dhSession.finaliseKeyAgreement(dhMessage.getContent());
            byte [] decrypted = _dhSession.decrypt(SERVER_SYNC_CIPHER_MODE,
                                                   SERVER_SYNC_CIPHER_NAME,
                                                   SERVER_SYNC_CIPHER_BLOCKSIZE,
                                                   encrypted);

            ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(decrypted);
            NestedBucketBuffer nestedBucket =
                NestedBucketBuffer.deserialize(kXRS_main, buffer);

            XrootdBucket clientX509Bucket =
                nestedBucket.getNestedBuckets().get(kXRS_x509);
            String clientX509 =
                ((StringBucket) clientX509Bucket).getContent();

            /* now it's time to verify the client's X509 certificate */
            List<X509Certificate> clientCerts =
                CertUtil.parseCerts(new StringReader(clientX509));

            X509Certificate proxyCert;

            if (clientCerts.size() > 1) {
                proxyCert = clientCerts.get(0);
            } else {
                throw new IllegalArgumentException("Could not parse user " +
                                                   "certificate from input stream!");
            }

            _logger.info("The proxy-cert has the following DN: {}",
                         proxyCert.getSubjectDN());

            /* need a serializable subject DN for cell communication */

            // TODO: Use the X500 principal directly
            GlobusPrincipal dn =
                new GlobusPrincipal(proxyCert.getSubjectX500Principal().getName());
            _subject.getPrincipals().add(dn);
            _logger.info("Issuer-DN: {}", proxyCert.getIssuerDN());

            X509Certificate[] proxyCertChain =
                clientCerts.toArray(new X509Certificate[0]);
            _subject.getPublicCredentials().add(proxyCertChain);

            _proxyValidator.validate(proxyCertChain,
                                     _trustedCerts.getCertificates(),
                                     _crls,
                                     _trustedCerts.getSigningPolicies());

            _challengeCipher.init(Cipher.DECRYPT_MODE, proxyCert.getPublicKey());

            XrootdBucket signedRTagBucket =
                nestedBucket.getNestedBuckets().get(kXRS_signed_rtag);
            byte[] signedRTag = ((RawBucket) signedRTagBucket).getContent();

            byte[] rTag = _challengeCipher.doFinal(signedRTag);
            String rTagString = new String(rTag, "ASCII");

            // check that the challenge sent in the previous step matches
            if (_challenge.equals(rTagString)) {
               _logger.info("signature of challenge tag ok. Challenge: " +
                            "{}, rTagString: {}", _challenge, rTagString);

               _finished = true;

               response = new OKResponse(request.getStreamID());
            } else {
               _logger.error("The challenge is {}, the serialized rTag is {}." +
                             "signature of challenge tag has been proven wrong!!",
                             _challenge, rTagString);
               response = new ErrorResponse(request.getStreamID(),
                                            kXR_InvalidRequest,
                                            "Client did not present correct" +
                                            "challenge response!");
            }



        } catch (InvalidKeyException ikex) {
            _logger.error("The key negotiated by DH key exchange appears to " +
                          "be invalid: {}", ikex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_InvalidRequest,
                                         "Could not decrypt client" +
                                         "information with negotiated key.");
        } catch (InvalidKeySpecException iksex) {
            _logger.error("DH key negotiation caused problems{}", iksex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_InvalidRequest,
                                         "Could not find key negotiation " +
                                         "parameters.");
        } catch (IOException ioex) {
            _logger.error("Could not deserialize main nested buffer {}", ioex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_IOError,
                                         "Could not decrypt encrypted " +
                                         "client message.");
        } catch (ProxyPathValidatorException ppvex) {
            _logger.error("Could not validate certificate path of client " +
                          "certificate: {}", ppvex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_NotAuthorized,
                                         "Your certificate's issuer is " +
                                         "not trusted.");
        } catch (GeneralSecurityException gssex) {
            _logger.error("Error during decrypting/server-side key exchange: {}",
                          gssex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_ServerError,
                                         "Error in server-side cryptographic " +
                                         "operations.");
        } catch (RuntimeException rtex) {
            _logger.error("Processing of kXRC_cert failed due to a bug: {}",
                          rtex);
            response = new ErrorResponse(request.getStreamID(),
                                         kXR_ServerError,
                                         "An error occured while processing " +
                                         "the cert request.");
        }

        return response;
    }

    /**
     * Build the server response to the kXGC_certReq request.
     * Such a response will include the signed challenge sent by the client,
     * a new challenge created by the server, the cryptoMode (typically SSL),
     * DH key exchange parameters, a list of supported ciphers, a list of
     * supported digests and a PEM-encoded host certificate.
     *
     * @param signedChallenge
     * @param newChallenge
     * @param cryptoMode
     * @param puk
     * @param supportedCiphers
     * @param supportedDigests
     * @param hostCertificate
     * @return List with the above parameters plus size in bytes of the bucket
     *         list.
     */
    private XrootdBucketContainer buildCertReqResponse(byte[] signedChallenge,
                                                       String newChallenge,
                                                       String cryptoMode,
                                                       byte [] puk,
                                                       String supportedCiphers,
                                                       String supportedDigests,
                                                       String hostCertificate) {
        int responseLength = 0;
        List<XrootdBucket> responseList = new ArrayList<XrootdBucket>();

        RawBucket signedRtagBucket =
            new RawBucket(BucketType.kXRS_signed_rtag, signedChallenge);
        StringBucket randomTagBucket = new StringBucket(kXRS_rtag, newChallenge);

        Map<BucketType, XrootdBucket> nestedBuckets =
            new EnumMap<BucketType, XrootdBucket>(BucketType.class);
        nestedBuckets.put(signedRtagBucket.getType(), signedRtagBucket);
        nestedBuckets.put(randomTagBucket.getType(), randomTagBucket);

        NestedBucketBuffer mainBucket = new NestedBucketBuffer(kXRS_main,
                                                               PROTOCOL,
                                                               kXGS_cert,
                                                               nestedBuckets);

        StringBucket cryptoBucket = new StringBucket(kXRS_cryptomod, CRYPTO_MODE);
        responseLength += cryptoBucket.getSize();
        responseList.add(cryptoBucket);
        responseLength += mainBucket.getSize();
        responseList.add(mainBucket);

        RawBucket dhPublicBucket = new RawBucket(kXRS_puk, puk);
        responseLength += dhPublicBucket.getSize();
        responseList.add(dhPublicBucket);

        StringBucket cipherBucket = new StringBucket(kXRS_cipher_alg,
                                                     supportedCiphers);
        responseLength += cipherBucket.getSize();
        responseList.add(cipherBucket);

        StringBucket digestBucket = new StringBucket(kXRS_md_alg,
                                                     supportedDigests);
        responseLength += digestBucket.getSize();
        responseList.add(digestBucket);

        StringBucket hostCertBucket =
            new StringBucket(kXRS_x509,
                             hostCertificate);
        responseLength += hostCertBucket.getSize();
        responseList.add(hostCertBucket);

        return new XrootdBucketContainer(responseList, responseLength);
    }

    /**
     * Generate a new challenge string to be used in server-client
     * communication
     * @return challenge string
     */
    private String generateChallengeString() {
        String result = "";
        byte[] challengeBytes = new byte[CHALLENGE_BYTES];

        /*
         * _random.nextBytes(...) can not be used, since this generates
         * signed bytes. Upon encoding as string, Java will map negative bytes
         * to 63 (ASCII 'A'). As this would affect the randomness of the
         * challenge string, use the following loop instead.
         */
        for (int i = 0; i < CHALLENGE_BYTES; i++) {
            challengeBytes[i] = (byte) _random.nextInt(Byte.MAX_VALUE);
        }

        try {
            result = new String(challengeBytes, "ASCII");
        } catch (UnsupportedEncodingException uee) {
            result = new String(challengeBytes);
        }

        return result;
    }

    /**
     * @return the protocol supported by this client. The protocol string also
     * contains metainformation such as the host-certificate subject hash.
     */
    @Override
    public String getProtocol() {
        /* hashed principals are cached in CertUtil */
        String subjectHash =
            CertUtil.computeMD5Hash(_hostCertificate.getIssuerX500Principal());

        String protocol = "&P=" + PROTOCOL + "," +
                          "v:" + PROTOCOL_VERSION + "," +
                          "c:" + CRYPTO_MODE + "," +
                          "ca:" + subjectHash;
        return protocol;
    }

    @Override
    public Subject getSubject() {
        return _subject;
    }

    @Override
    public boolean isAuthenticationCompleted() {
        return _finished;
    }

    @Override
    public boolean isStrongAuthentication() {
        return true;
    }
}
