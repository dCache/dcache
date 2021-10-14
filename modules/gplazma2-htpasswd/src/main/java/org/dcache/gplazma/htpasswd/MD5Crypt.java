/*

   MD5Crypt.java

   Created: 3 November 1999
   Java Port By: Jonathan Abbey, jonabbey@arlut.utexas.edu
   Original C Version:
   ----------------------------------------------------------------------------
   "THE BEER-WARE LICENSE" (Revision 42):
   <phk@login.dknet.dk> wrote this file.  As long as you retain this notice you
   can do whatever you want with this stuff. If we meet some day, and you think
   this stuff is worth it, you can buy me a beer in return.   Poul-Henning Kamp
   ----------------------------------------------------------------------------

   This Java Port is

     Copyright (c) 1999-2008 The University of Texas at Austin.

     All rights reserved.

     Redistribution and use in source and binary form are permitted
     provided that distributions retain this entire copyright notice
     and comment. Neither the name of the University nor the names of
     its contributors may be used to endorse or promote products
     derived from this software without specific prior written
     permission. THIS SOFTWARE IS PROVIDED "AS IS" AND WITHOUT ANY
     EXPRESS OR IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE
     IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
     PARTICULAR PURPOSE.

*/

package org.dcache.gplazma.htpasswd;

import java.security.MessageDigest;
import org.dcache.gplazma.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines a method, {@link MD5Crypt#crypt(java.lang.String, java.lang.String) crypt()},
 * which takes a password and a salt string and generates an OpenBSD/FreeBSD/Linux-compatible
 * md5-encoded password entry.
 */
public final class MD5Crypt {

    private static final Logger LOGGER = LoggerFactory.getLogger(MD5Crypt.class);

    private static final String itoa64 = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private static final String to64(long v, int size) {
        StringBuilder result = new StringBuilder();

        while (--size >= 0) {
            result.append(itoa64.charAt((int) (v & 0x3f)));
            v >>>= 6;
        }

        return result.toString();
    }

    private static final void clearbits(byte bits[]) {
        for (int i = 0; i < bits.length; i++) {
            bits[i] = 0;
        }
    }

    /**
     * convert an encoded unsigned byte value into a int with the unsigned value.
     */

    private static final int bytes2u(byte inp) {
        return (int) inp & 0xff;
    }

    private static MessageDigest getMD5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (java.security.NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * <p>This method actually generates a OpenBSD/FreeBSD/Linux PAM compatible md5-encoded
     * password
     * hash from a plaintext password and a salt.</p>
     * <p/>
     * <p>The resulting string will be in the form '$1$&lt;salt&gt;$&lt;hashed mess&gt;</p>
     *
     * @param password Plaintext password
     * @param salt     A short string to use to randomize md5.  May start with $1$, which will be
     *                 ignored.  It is explicitly permitted to pass a pre-existing MD5Crypt'ed
     *                 password entry as the salt.  crypt() will strip the salt chars out properly.
     * @return An OpenBSD/FreeBSD/Linux-compatible md5-hashed password field.
     */

    public static final String crypt(String password, String salt) {
        return MD5Crypt.crypt(password, salt, "$1$");
    }

    /**
     * <p>This method actually generates an Apache MD5 compatible md5-encoded password hash from a
     * plaintext password and a salt.</p>
     * <p/>
     * <p>The resulting string will be in the form '$apr1$&lt;salt&gt;$&lt;hashed mess&gt;</p>
     *
     * @param password Plaintext password
     * @param salt     A short string to use to randomize md5.  May start with $apr1$, which will be
     *                 ignored.  It is explicitly permitted to pass a pre-existing MD5Crypt'ed
     *                 password entry as the salt.  crypt() will strip the salt chars out properly.
     * @return An Apache-compatible md5-hashed password string.
     */

    public static final String apacheCrypt(String password, String salt) {
        return MD5Crypt.crypt(password, salt, "$apr1$");
    }

    /**
     * <p>This method generates a md5-encoded password hash from a plaintext password, a salt, and
     * a
     * magic string.</p>
     * <p/>
     * <p>There are two magic strings that make sense to use here.. '$1$' is the magic string used
     * by the FreeBSD/Linux/OpenBSD MD5Crypt algorithm, and '$apr1$' is the magic string used by the
     * Apache MD5Crypt algorithm.</p>
     * <p/>
     * <p>The resulting string will be in the form '&lt;magic&gt;&lt;salt&gt;$&lt;hashed
     * mess&gt;</p>
     *
     * @param password Plaintext password
     * @param salt     A short string to use to randomize md5.  May start with the magic string,
     *                 which will be ignored.  It is explicitly permitted to pass a pre-existing
     *                 MD5Crypt'ed password entry as the salt.  crypt() will strip the salt chars
     *                 out properly.
     * @param magic    Either "$apr1$" or "$1$", which controls whether we are doing Apache-style or
     *                 FreeBSD-style md5Crypt.
     * @return An md5-hashed password string.
     */

    public static final String crypt(String password, String salt, String magic) {
        /* This string is magic for this algorithm.  Having it this way,
         * we can get get better later on */

        byte finalState[];
        MessageDigest ctx, ctx1;
        long l;

        /* -- */

        /* Refine the Salt first */

        /* If it starts with the magic string, then skip that */

        if (salt.startsWith(magic)) {
            salt = salt.substring(magic.length());
        }

        /* It stops at the first '$', max 8 chars */

        if (salt.indexOf('$') != -1) {
            salt = salt.substring(0, salt.indexOf('$'));
        }

        if (salt.length() > 8) {
            salt = salt.substring(0, 8);
        }

        ctx = getMD5();

        ctx.update(password
              .getBytes());    // The password first, since that is what is most unknown
        ctx.update(magic.getBytes());    // Then our magic string
        ctx.update(salt.getBytes());    // Then the raw salt

        /* Then just as many characters of the MD5(pw,salt,pw) */

        ctx1 = getMD5();
        ctx1.update(password.getBytes());
        ctx1.update(salt.getBytes());
        ctx1.update(password.getBytes());
        finalState = ctx1.digest();

        for (int pl = password.length(); pl > 0; pl -= 16) {
            ctx.update(finalState, 0, pl > 16 ? 16 : pl);
        }

        /* the original code claimed that finalState was being cleared
           to keep dangerous bits out of memory, but doing this is also
           required in order to get the right output. */

        clearbits(finalState);

        /* Then something really weird... */

        for (int i = password.length(); i != 0; i >>>= 1) {
            if ((i & 1) != 0) {
                ctx.update(finalState, 0, 1);
            } else {
                ctx.update(password.getBytes(), 0, 1);
            }
        }

        finalState = ctx.digest();

        /*
         * and now, just to make sure things don't run too fast
         * On a 60 Mhz Pentium this takes 34 msec, so you would
         * need 30 seconds to build a 1000 entry dictionary...
         *
         * (The above timings from the C version)
         */

        for (int i = 0; i < 1000; i++) {
            ctx1.reset();

            if ((i & 1) != 0) {
                ctx1.update(password.getBytes());
            } else {
                ctx1.update(finalState, 0, 16);
            }

            if ((i % 3) != 0) {
                ctx1.update(salt.getBytes());
            }

            if ((i % 7) != 0) {
                ctx1.update(password.getBytes());
            }

            if ((i & 1) != 0) {
                ctx1.update(finalState, 0, 16);
            } else {
                ctx1.update(password.getBytes());
            }

            finalState = ctx1.digest();
        }

        /* Now make the output string */

        StringBuilder result = new StringBuilder();

        result.append(magic);
        result.append(salt);
        result.append("$");

        l = (bytes2u(finalState[0]) << 16) | (bytes2u(finalState[6]) << 8) | bytes2u(
              finalState[12]);
        result.append(to64(l, 4));

        l = (bytes2u(finalState[1]) << 16) | (bytes2u(finalState[7]) << 8) | bytes2u(
              finalState[13]);
        result.append(to64(l, 4));

        l = (bytes2u(finalState[2]) << 16) | (bytes2u(finalState[8]) << 8) | bytes2u(
              finalState[14]);
        result.append(to64(l, 4));

        l = (bytes2u(finalState[3]) << 16) | (bytes2u(finalState[9]) << 8) | bytes2u(
              finalState[15]);
        result.append(to64(l, 4));

        l = (bytes2u(finalState[4]) << 16) | (bytes2u(finalState[10]) << 8) | bytes2u(
              finalState[5]);
        result.append(to64(l, 4));

        l = bytes2u(finalState[11]);
        result.append(to64(l, 2));

        /* Don't leave anything around in vm they could use. */
        clearbits(finalState);

        return result.toString();
    }

    /**
     * This method tests a plaintext password against a md5Crypt'ed hash and returns true if the
     * password matches the hash.
     * <p/>
     * This method will work properly whether the hashtext was crypted using the default FreeBSD
     * md5Crypt algorithm or the Apache md5Crypt variant.
     *
     * @param plaintextPass The plaintext password text to test.
     * @param md5CryptText  The Apache or FreeBSD-md5Crypted hash used to authenticate the
     *                      plaintextPass.
     * @throws AuthenticationException if the md5CryptText is badly formed.
     */
    public static final boolean verifyPassword(String plaintextPass, String md5CryptText)
          throws AuthenticationException {
        if (md5CryptText.startsWith("$1$")) {
            return md5CryptText.equals(MD5Crypt.crypt(plaintextPass, md5CryptText));
        } else if (md5CryptText.startsWith("$apr1$")) {
            return md5CryptText.equals(MD5Crypt.apacheCrypt(plaintextPass, md5CryptText));
        } else {
            LOGGER.error("Bad entry in file: hash does not start '$1$' or '$apr1$': {}",
                  md5CryptText);
            throw new AuthenticationException("bad hash in file");
        }
    }
}
