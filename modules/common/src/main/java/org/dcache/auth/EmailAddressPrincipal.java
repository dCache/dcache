package org.dcache.auth;

import java.io.Serializable;
import java.security.Principal;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @since 2.16
 */
public class EmailAddressPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = -5091924321331479809L;
    // Pattern based on
    // http://stackoverflow.com/questions/624581/what-is-the-best-java-email-address-validation-method
    private static final Pattern EMAIL_PATTERN =
        Pattern.compile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\])|(([a-zA-Z\\-0-9]+\\.)+[a-zA-Z]{2,}))$", Pattern.CASE_INSENSITIVE);

    private final String _email;

    public static boolean isValid(String email)
    {
        return EMAIL_PATTERN.matcher(email).matches();
    }

    public EmailAddressPrincipal(String email)
    {
        checkArgument(EMAIL_PATTERN.matcher(email).matches(), "Not a valid email address");
        _email = email;
    }

    @Override
    public String getName()
    {
        return _email;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmailAddressPrincipal)) {
            return false;
        }

        EmailAddressPrincipal that = (EmailAddressPrincipal) o;

        return _email.equals(that._email);
    }

    @Override
    public int hashCode()
    {
        return _email.hashCode();
    }

    @Override
    public String toString()
    {
        return "EmailAddressPrincipal[" + _email + ']';
    }
}
