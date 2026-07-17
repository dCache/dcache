/*
 * Copyright (c) 2013 ICM Uniwersytet Warszawski All rights reserved.
 * See LICENCE.txt file for licensing information.
 */
package eu.emi.security.authn.x509.helpers.trust;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.x500.X500Principal;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1OutputStream;
import org.bouncycastle.asn1.ASN1String;
import org.bouncycastle.asn1.DERBMPString;
import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.DERPrintableString;
import org.bouncycastle.asn1.DERT61String;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.DERUniversalString;
import org.bouncycastle.asn1.DERVisibleString;
import org.bouncycastle.asn1.x500.AttributeTypeAndValue;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;

import eu.emi.security.authn.x509.helpers.CertificateHelpers;

/**
 * Several static methods helping to mangle truststore file paths in openssl style.
 * 
 * @author K. Benedyczak
 */
public class OpensslTruststoreHelper
{
	public static final String CERT_REGEXP = "^([0-9a-fA-F]{8})\\.[\\d]+$";
	
	/**
	 * @param certLocation certificate location
	 * @param suffix either '.namespaces' or '.signing_policy' (other will work but rather doesn't make sense)
	 * @return A proper name of a namespaces or signing policy file for the given base
	 * path of CA certificate. 
	 */
	public static String getNsFile(String certLocation, String suffix)
	{
		String fileHash = getFileHash(certLocation, CERT_REGEXP);
		if (fileHash == null)
			return null;
		File f = new File(certLocation);
		String parent = f.getParent();
		if (parent == null)
			parent = ".";
		return parent + File.separator + fileHash + suffix;
	}
	
	public static String getFileHash(String path, String regexp)
	{
		File f = new File(path);
		String name = f.getName();
		Pattern pattern = Pattern.compile(regexp);
		Matcher m = pattern.matcher(name);
		if (!m.matches())
			return null;
		return m.group(1);
	}

	public static Collection<File> getFilesWithRegexp(String regexp, File directory)
	{
		final Pattern pattern = Pattern.compile(regexp);
		
		return FileUtils.listFiles(directory, new IOFileFilter()
		{
			@Override
			public boolean accept(File dir, String name)
			{
				return pattern.matcher(name).matches();
			}
			
			@Override
			public boolean accept(File file)
			{
				return accept(null, file.getName());
			}
		}, null);
	}
	
	public static String getOpenSSLCAHash(X500Principal name, boolean openssl1Mode)
	{
		return openssl1Mode ? getOpenSSLCAHashNew(name) : getOpenSSLCAHashOld(name);
	}
	
	/**
	 * Generates the hex hash of the DN used by openssl to name the CA
	 * certificate files. The hash is actually the hex of 8 least
	 * significant bytes of a MD5 digest of the the ASN.1 encoded DN.
	 * 
	 * @param name the DN to hash.
	 * @return the 8 character string of the hexadecimal MD5 hash.
	 */
	private static String getOpenSSLCAHashOld(X500Principal name)
	{
		byte[] bytes = name.getEncoded();
		MD5Digest digest = new MD5Digest();
		digest.update(bytes, 0, bytes.length);
		byte output[] = new byte[digest.getDigestSize()];
		digest.doFinal(output, 0);
		
		String ret = String.format("%02x%02x%02x%02x", output[3] & 0xFF,
				output[2] & 0xFF, output[1] & 0xFF, output[0] & 0xFF);
		return ret;
	}
	
	/**
	 * Generates the hex hash of the DN used by openssl 1.0.0 and above to name the CA
	 * certificate files. The hash is actually the hex of 8 least
	 * significant bytes of a SHA1 digest of the the ASN.1 encoded DN after normalization.
	 * <p>
	 * The normalization is performed as follows:
	 * all strings are converted to UTF8, leading, trailing and multiple spaces collapsed, 
	 * converted to lower case and the leading SEQUENCE header is removed.
	 * 
	 * @param name the DN to hash.
	 * @return the 8 character string of the hexadecimal MD5 hash.
	 */
	private static String getOpenSSLCAHashNew(X500Principal name)
	{
		byte[] bytes;
		try
		{
			RDN[] c19nrdns = getNormalizedRDNs(name);
			bytes = encodeWithoutSeqHeader(c19nrdns);
		} catch (IOException e)
		{
			throw new IllegalArgumentException("Can't parse the input DN", e);
		}
		Digest digest = new SHA1Digest();
		digest.update(bytes, 0, bytes.length);
		byte output[] = new byte[digest.getDigestSize()];
		digest.doFinal(output, 0);
		
		return String.format("%02x%02x%02x%02x", output[3] & 0xFF,
				output[2] & 0xFF, output[1] & 0xFF, output[0] & 0xFF);	
	}
	
	public static RDN[] getNormalizedRDNs(X500Principal name) throws IOException
	{
		X500Name dn = CertificateHelpers.toX500Name(name);
		RDN[] rdns = dn.getRDNs();
		RDN[] c19nrdns = new RDN[rdns.length];
		int i=0;
		for (RDN rdn: rdns)
		{
			AttributeTypeAndValue[] atvs = rdn.getTypesAndValues();
			sortAVAs(atvs);
			AttributeTypeAndValue[] c19natvs = new AttributeTypeAndValue[atvs.length];
			for (int j=0; j<atvs.length; j++)
			{
				c19natvs[j] = normalizeStringAVA(atvs[j]);
			}
			c19nrdns[i++] = new RDN(c19natvs);
		}
		return c19nrdns;
	}
	
	private static void sortAVAs(AttributeTypeAndValue[] atvs) throws IOException
	{
		for (int i=0; i<atvs.length; i++)
			for (int j=i+1; j<atvs.length; j++)
			{
				if (memcmp(atvs[i].getEncoded(), atvs[j].getEncoded()) < 0)
				{
					AttributeTypeAndValue tmp = atvs[i];
					atvs[i] = atvs[j];
					atvs[j] = tmp;
				}
			}
	}
	
	private static int memcmp(byte[] a, byte[] b)
	{
		int min = a.length > b.length ? b.length : a.length;
		for (int i=0; i<min; i++)
			if (a[i] < b[i])
				return -1;
			else if (a[i] > b[i])
				return 1;
		return a.length - b.length;
	}
	
	private static AttributeTypeAndValue normalizeStringAVA(AttributeTypeAndValue src)
	{
		ASN1Encodable srcVal = src.getValue();
		if (	!((srcVal instanceof DERPrintableString) ||
			(srcVal instanceof DERUTF8String) ||
			(srcVal instanceof DERIA5String) ||
			(srcVal instanceof DERBMPString) ||
			(srcVal instanceof DERUniversalString) ||
			(srcVal instanceof DERT61String) ||
			(srcVal instanceof DERVisibleString)))
			return src;
		ASN1String srcString = (ASN1String) srcVal;
		String value = srcString.getString();
		value = value.trim();
		value = value.replaceAll("[ \t\n\f][ \t\n\f]+", " ");
		value = value.toLowerCase();
		DERUTF8String newValue = new DERUTF8String(value);
		return new AttributeTypeAndValue(src.getType(), newValue);
	}
	
	private static byte[] encodeWithoutSeqHeader(RDN[] rdns) throws IOException
	{
	        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
	        ASN1OutputStream      aOut = ASN1OutputStream.create(bOut);

		for (RDN rdn: rdns)
		{
			aOut.writeObject(rdn);
		}
		aOut.close();
		return bOut.toByteArray();
	}
}
