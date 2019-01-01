package org.dcache.macaroons;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import com.github.nitram509.jmacaroons.MacaroonsConstants;
import com.github.nitram509.jmacaroons.MacaroonsVerifier;
import com.github.nitram509.jmacaroons.NotDeSerializableException;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;

import org.dcache.util.Strings;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.macaroons.CaveatValues.*;

/**
 * Class that acts as a facade to the Macaroon library.
 */
public class MacaroonProcessor
{
    private static final Logger LOG  = LoggerFactory.getLogger(MacaroonProcessor.class);
    private static final int SECRET_ID_LENGTH = 8;
    // In Base64, every 3 bytes is represented as 4 characters.
    private static final int SECRET_ID_LENGTH_BYTES = SECRET_ID_LENGTH * 3 / 4;

    // 48 bit: collision p=0.01 after 2.4x10^6 values, p=0.5 after 2x10^7.
    private static final int ISSUE_ID_LENGTH_BYTES = 6;

    private final SecureRandom random = new SecureRandom();

    private SecretHandler _secretHandler;

    class CaveatMacaroonsBuilder extends MacaroonsBuilder
    {
        CaveatMacaroonsBuilder(String location, byte[] secretKey, String identifier)
        {
            super(location, secretKey, identifier);
        }

        void addCaveat(CaveatType type, Object value)
        {
            addCaveat(new Caveat(type, String.valueOf(value)));
        }

        void addCaveat(Caveat caveat)
        {
            add_first_party_caveat(caveat.toString());
        }
    }

    public class MacaroonBuildResult
    {
        private final String macaroon;
        private final String id;

        public MacaroonBuildResult(Macaroon macaroon, String id)
        {
            this.macaroon = macaroon.serialize();
            this.id = id;
        }

        public String getMacaroon()
        {
            return macaroon;
        }

        public String getId()
        {
            return id;
        }
    }

    @Required
    public void setSecretHandler(SecretHandler supplier)
    {
        _secretHandler = supplier;
    }

    private IdentifiedSecret newSecret()
    {
        byte[] secret = new byte[MacaroonsConstants.MACAROON_SUGGESTED_SECRET_LENGTH];
        random.nextBytes(secret);

        byte[] rawId = Arrays.copyOf(Hashing.sha256().hashBytes(secret).asBytes(), SECRET_ID_LENGTH_BYTES);
        String identifier = Base64.getMimeEncoder().withoutPadding().encodeToString(rawId);

        return new IdentifiedSecret(identifier, secret);
    }

    private String newIssueId()
    {
        byte[] raw = new byte[ISSUE_ID_LENGTH_BYTES];
        random.nextBytes(raw);
        return Base64.getMimeEncoder().withoutPadding().encodeToString(raw);
    }

    public MacaroonBuildResult buildMacaroon(Instant expiry, MacaroonContext identity,
            Collection<Caveat> userSuppliedCaveats) throws InvalidCaveatException, InternalErrorException
    {
        IdentifiedSecret secret;
        try {
            secret = _secretHandler.secretExpiringAfter(expiry, this::newSecret);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            throw new InternalErrorException(msg, e);
        }

        CaveatMacaroonsBuilder builder = new CaveatMacaroonsBuilder(identity.getPath().toString(),
                secret.getSecret(), secret.getIdentifier());

        String issueId = newIssueId();
        builder.addCaveat(CaveatType.ISSUE_ID, issueId);
        builder.addCaveat(CaveatType.IDENTITY, asIdentityCaveatValue(identity));
        builder.addCaveat(CaveatType.BEFORE, expiry);

        identity.getRoot().ifPresent(p -> builder.addCaveat(CaveatType.ROOT, p));
        identity.getHome().ifPresent(p -> builder.addCaveat(CaveatType.HOME, p));
        identity.getPath().ifPresent(p -> builder.addCaveat(CaveatType.PATH, p));
        identity.getMaxUpload().ifPresent(p -> builder.addCaveat(CaveatType.MAX_UPLOAD, p));

        identity.getAllowedActivities()
                .map(CaveatValues::asActivityCaveatValue)
                .ifPresent(a -> builder.addCaveat(CaveatType.ACTIVITY, a));

        userSuppliedCaveats.stream().forEach(builder::addCaveat);

        Macaroon macaroon = builder.getMacaroon();
        String id = MacaroonContext.buildId(macaroon.signature, issueId);
        return new MacaroonBuildResult(macaroon, id);
    }

    public MacaroonContext expandMacaroon(String serialisedMacaroon, InetAddress clientAddress)
            throws InvalidMacaroonException
    {
        LOG.trace("Received macaroon validate message");

        Macaroon macaroon = MacaroonsBuilder.deserialize(serialisedMacaroon);

        byte[] secret = _secretHandler.findSecret(macaroon.identifier);
        checkArgument(secret != null, "Unable to find secret for macaroon [%s]",
                MacaroonContext.buildId(macaroon.signature, "-"));

        MacaroonsVerifier verifier = new MacaroonsVerifier(macaroon);

        ClientIPCaveatVerifier clientIPVerifier = new ClientIPCaveatVerifier(clientAddress);
        verifier.satisfyGeneral(clientIPVerifier);

        ContextExtractingCaveatVerifier contextExtractor = new ContextExtractingCaveatVerifier();
        verifier.satisfyGeneral(contextExtractor);

        if (!verifier.isValid(secret)) {
            StringBuilder error = new StringBuilder("Invalid macaroon [")
                    .append(MacaroonContext.buildId(macaroon.signature, "-"))
                    .append(']');

            Strings.combine(clientIPVerifier.getError(), " and ", contextExtractor.getError())
                    .ifPresent(msg -> error.append(": ").append(msg));

            throw new InvalidMacaroonException(error.toString());
        }

        MacaroonContext context = contextExtractor.getContext();
        context.setSignature(macaroon.signature);
        return context;
    }

    /**
     * Return whether the supplied token is formatted as a Macaroon.  This
     * does not check whether the token is valid.
     */
    public boolean isMacaroon(String token)
    {
        try {
            MacaroonsBuilder.deserialize(token);
            return true;
        } catch (NegativeArraySizeException | ArrayIndexOutOfBoundsException | NotDeSerializableException e) {
            // FIXME: jmacaroons tends to throw "random" RuntimeException exceptions on bad input
            return false;
        }
    }
}
