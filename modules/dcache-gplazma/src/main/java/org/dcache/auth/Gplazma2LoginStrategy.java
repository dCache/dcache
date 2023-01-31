package org.dcache.auth;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import dmg.cells.nucleus.CellCommandListener;
import java.io.File;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.MultiTargetedRestriction;
import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.GPlazma;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.monitor.LoginResult;
import org.dcache.gplazma.monitor.LoginResultPrinter;
import org.dcache.gplazma.monitor.RecordingLoginMonitor;
import org.dcache.util.Args;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * A LoginStrategy that delegates login requests to an instance of org.dcache.gplazma.GPlazma.
 */
public class Gplazma2LoginStrategy implements LoginStrategy, CellCommandListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(Gplazma2LoginStrategy.class);

    private static final Set<Activity> ALLOWED_UPLOAD_DIR_ACTIVITIES
          = Set.of(Activity.DELETE, Activity.UPLOAD, Activity.MANAGE, Activity.READ_METADATA,
          Activity.UPDATE_METADATA);

    // These are principals defined outside of dCache.
    private static final Set<Class<?>> EXTERNAL_AUTHENTICATION_INPUT = Set.of(
          KerberosPrincipal.class);
    private static final Set<Class<?>> EXTERNAL_AUTHENTICATION_OUTPUT = Set.of();

    /**
     * Principal classes allowed as input to gPlazma.
     */
    private static final Set<Class<?>> AUTHENTICATION_INPUT;

    /**
     * Principal classes allowed in the response from gPlazma.
     */
    private static final Set<Class<?>> AUTHENTICATION_OUTPUT;

    private String _configurationFile;
    private GPlazma _gplazma;
    private Function<FsPath, PrefixRestriction> _createPrefixRestriction = PrefixRestriction::new;
    private Optional<String> _uploadPath = Optional.empty();

    static {
        Stopwatch reflectionTimer = Stopwatch.createStarted();
        Reflections principalPackages = new Reflections("org.dcache.auth",
              "org.globus.gsi.gssapi.jaas");
        AUTHENTICATION_INPUT = principalPackages.getTypesAnnotatedWith(AuthenticationInput.class);
        AUTHENTICATION_INPUT.addAll(EXTERNAL_AUTHENTICATION_INPUT);
        LOGGER.debug("AUTHENTICATION_INPUT: {}", AUTHENTICATION_INPUT);

        AUTHENTICATION_OUTPUT = principalPackages.getTypesAnnotatedWith(AuthenticationOutput.class);
        AUTHENTICATION_OUTPUT.addAll(EXTERNAL_AUTHENTICATION_OUTPUT);
        LOGGER.debug("AUTHENTICATION_OUTPUT: {}", AUTHENTICATION_OUTPUT);

        LOGGER.debug("gPlazma2 principal scanning completed: {}", reflectionTimer);
    }

    @Required
    public void setConfigurationFile(String configurationFile) {
        if ((configurationFile == null) || (configurationFile.length() == 0)) {
            throw new IllegalArgumentException(
                  "configuration file argument wasn't specified correctly");
        } else if (!new File(configurationFile).exists()) {
            throw new IllegalArgumentException(
                  "configuration file does not exists at " + configurationFile);
        }
        _configurationFile = configurationFile;
    }

    @Required
    public void setGplazma(GPlazma gplazma) {
        _gplazma = requireNonNull(gplazma);
    }

    public String getConfigurationFile() {
        return _configurationFile;
    }

    /*
     *  REVISIT  2023/01/23
     *
     *  This is a provisional solution to
     *  GH File uploads with gfal using roots protocol with tokens fails #6952
     *  https://github.com/dCache/dcache/issues/6952.
     *
     *  It relies on the fact that only the OIDC and SciToken plugins currently make
     *  use of the MultiTargetedRestriction.  This may change in the future, and
     *  could cause problems.
     *
     *  A more general solution for providing permissions on an upload directory
     *  is advisable.
     */
    private LoginReply convertLoginReply(org.dcache.gplazma.LoginReply gPlazmaLoginReply) {
        Set<Object> sessionAttributes = gPlazmaLoginReply.getSessionAttributes();
        Set<LoginAttribute> loginAttributes = new HashSet<>();
        Set<MultiTargetedRestriction> mtRestrictions = new HashSet<>();
        Set<FsPath> userRoots = new HashSet<>();

        findAttributesAndUserRoots(sessionAttributes, loginAttributes, mtRestrictions, userRoots);

        /*
         *  REVISIT 2023/01/23
         *
         *  When LoginReply.getRestriction() is called, Restrictions.concat(restrictions)
         *  may be called.  This means that a composite restriction is created, in which
         *  any restriction within it can veto an activity.  With MultiTargetedRestrictions,
         *  however, we do not want their potentially stronger constraints vetoing a
         *  PrefixRestriction containing permissions on an upload directory.
         *
         *  The solution here is not to add Prefix restrictions on the user ROOT and/or
         *  UPLOAD dir if there are MultiTargetRestrictions, but rather to replace the
         *  existing restriction with a new one also containing the upload directory
         *  authorisation.
         */
        if (mtRestrictions.isEmpty()) {
            userRoots.stream().map(_createPrefixRestriction).forEach(loginAttributes::add);
        } else {
            handleMultiTargetedRestrictions(userRoots, mtRestrictions, loginAttributes);
        }

        Subject replyUser = filterPrincipals(gPlazmaLoginReply.getSubject(),
              AUTHENTICATION_OUTPUT, "LoginReply");

        return new LoginReply(replyUser, loginAttributes);
    }

    private Subject filterPrincipals(Subject in, Collection<Class<?>> allowed,
          String name) {
        Set<Principal> inPrincipals = in.getPrincipals();

        Set<Principal> outPrincipals = inPrincipals.stream()
              .filter(p -> allowed.contains(p.getClass()))
              .collect(Collectors.toSet());

        if (outPrincipals.size() == inPrincipals.size()) {
            return in;
        }

        LOGGER.debug("Filtered principals in {}: {}", name,
              Sets.difference(inPrincipals, outPrincipals));

        return new Subject(false, outPrincipals, in.getPublicCredentials(),
              in.getPrivateCredentials());
    }

    private void findAttributesAndUserRoots(Set<Object> sessionAttributes,
          Set<LoginAttribute> loginAttributes, Set<MultiTargetedRestriction> mtRestrictions,
          Set<FsPath> userRoots) {
        for (Object attr : sessionAttributes) {
            if (attr instanceof MultiTargetedRestriction) {
                mtRestrictions.add((MultiTargetedRestriction) attr);
            } else if (attr instanceof LoginAttribute) {
                loginAttributes.add((LoginAttribute) attr);
                if (attr instanceof RootDirectory) {
                    RootDirectory rootDir = (RootDirectory) attr;
                    String root = rootDir.getRoot();
                    if (!root.equals("/")) {
                        userRoots.add(FsPath.create(root));
                    }
                }
            }
        }
    }

    private void handleMultiTargetedRestrictions(Set<FsPath> userRoots,
          Set<MultiTargetedRestriction> mtRestrictions,
          Set<LoginAttribute> loginAttributes) {
        Collection<Authorisation> uploadAuthorizations =
              _uploadPath.map(up -> userRoots.stream().map(r -> r.resolve(up))
                    .map(path -> new Authorisation(ALLOWED_UPLOAD_DIR_ACTIVITIES, path))
                    .collect(Collectors.toList())).orElseGet(List::of);

        mtRestrictions.stream().map(r -> r.alsoAuthorising(uploadAuthorizations))
              .forEach(loginAttributes::add);
    }

    @Override
    public LoginReply login(Subject subject) throws CacheException {
        Subject filteredSubject = filterPrincipals(subject, AUTHENTICATION_INPUT,
              "login subject");

        try {
            org.dcache.gplazma.LoginReply reply = _gplazma.login(filteredSubject);
            return convertLoginReply(reply);
        } catch (AuthenticationException e) {
            LOGGER.info("Login failed: {}", e.getMessage());
            // We deliberately hide the reason why the login failed from the
            // rest of dCache.  This is to prevent a brute-force attack
            // discovering whether certain user accounts exist.
            throw new PermissionDeniedCacheException("login failed");
        }
    }

    @Override
    public Principal map(Principal principal) throws CacheException {
        try {
            return _gplazma.map(principal);
        } catch (NoSuchPrincipalException e) {
            return null;
        }
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException {
        try {
            return _gplazma.reverseMap(principal);
        } catch (NoSuchPrincipalException e) {
            return Collections.emptySet();
        }
    }

    public static final String fh_explain_login =
          "This command runs a test login with the supplied principals\n" +
                "The result is tracked and an explanation is provided of how \n" +
                "the result was obtained.\n\n" +
                "Examples:\n" +
                "  explain login \"dn:/C=DE/O=GermanGrid/OU=DESY/CN=testUser\" fqan:/test\n" +
                "  explain login username:testuser\n";
    public static final String hh_explain_login = "<principal> [<principal> ...] # explain the result of login";

    public String ac_explain_login_$_1_99(Args args) {
        Subject subject = Subjects.subjectFromArgs(args.getArguments());
        RecordingLoginMonitor monitor = new RecordingLoginMonitor();
        try {
            _gplazma.login(subject, monitor);
        } catch (AuthenticationException e) {
            // ignore exception: we'll show this in the explanation.
        }

        LoginResult result = monitor.getResult();
        LoginResultPrinter printer = new LoginResultPrinter(result);
        return printer.print();
    }

    @Required
    public void setUploadPath(String s) {
        _uploadPath = Optional.ofNullable(Strings.emptyToNull(s));
        /*
         *  The case where the upload directory is relative
         *  is already handled by the initialized value of the function.
         */
        if (_uploadPath.isPresent() && s.startsWith("/")) {
            FsPath uploadPath = FsPath.create(s);
            _createPrefixRestriction = path -> new PrefixRestriction(path, uploadPath);
        }
    }
}