package diskCacheV111.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.security.auth.Subject;
import org.dcache.auth.Subjects;

public class CheckStagePermission {
    private File _stageConfigFile;
    private Long _lastTimeReadingStageConfigFile = 0L;
    private List<Pattern[]> _regexList;
    private final boolean _isEnabled;

    private final ReadWriteLock _fileReadWriteLock = new ReentrantReadWriteLock();
    private final Lock _fileReadLock = _fileReadWriteLock.readLock();
    private final Lock _fileWriteLock = _fileReadWriteLock.writeLock();

    public CheckStagePermission(String stageConfigurationFilePath) {
        if ( stageConfigurationFilePath == null || stageConfigurationFilePath.length() == 0 ) {
            _isEnabled = false;
            return;
        }

        _stageConfigFile = new File(stageConfigurationFilePath);
        _isEnabled = true;
    }

    /**
     * Check whether staging is allowed for a particular subject.
     *
     * @param subject The subject
     * @return true if and only if the subject is allowed to perform
     * staging
     */
    public boolean canPerformStaging(Subject subject)
        throws PatternSyntaxException, IOException
    {
        if (!_isEnabled || Subjects.isRoot(subject))
            return true;

        try {
            String dn = Subjects.getDn(subject);
            Collection<String> fqans = Subjects.getFqans(subject);

            if (dn == null) {
                dn = "";
            }

            if (fqans.isEmpty()) {
                return canPerformStaging(dn, "");
            } else {
                for (String fqan: fqans) {
                    if (canPerformStaging(dn, fqan))
                        return true;
                }
                return false;
            }
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("Subject has multible DNs");
        }
    }

    /**
    * Check whether staging is allowed for the user with given DN and FQAN.
    *
    * @param  dn user's Distinguished Name
    * @param  fqan user's Fully Qualified Attribute Name
    * @return true if the user is allowed to perform staging
    * @throws PatternSyntaxException
    * @throws IOException
    */
    public boolean canPerformStaging(String dn, FQAN fqan) throws PatternSyntaxException, IOException {

        String fqanStr;
        if ( fqan != null ) {
            fqanStr = fqan.toString();
        } else {
            fqanStr = "";
        }

        return canPerformStaging(dn, fqanStr);
    }

    /**
    * Check whether staging is allowed for the user with given DN and FQAN.
    *
    * @param  dn user's Distinguished Name
    * @param  fqan user's Fully Qualified Attribute Name
    * @return true if the user is allowed to perform staging
    * @throws PatternSyntaxException
    * @throws IOException
    */
    public boolean canPerformStaging(String dn, String fqan) throws PatternSyntaxException, IOException {

        if ( !_isEnabled )
            return true;

        if ( !_stageConfigFile.exists() ) {
            //if file does not exist, staging is denied for all users
            return false;
        }

        if ( fileNeedsRereading() )
            rereadConfig();

        if (fqan==null) fqan="";

        return userMatchesPredicates(dn, fqan);
    }

    /**
    * Reread the contents of the configuration file.
    * @throws IOException
    * @throws PatternSyntaxException
    */
    void rereadConfig() throws PatternSyntaxException, IOException {
        try {
            _fileWriteLock.lock();
            if ( fileNeedsRereading() ) {
                BufferedReader reader = new BufferedReader(new FileReader(_stageConfigFile));
                _regexList = readStageConfigFile(reader);
                _lastTimeReadingStageConfigFile = System.currentTimeMillis();
            }
        } finally {
            _fileWriteLock.unlock();
        }
    }

    /**
    * Check whether the stageConfigFile needs rereading.
    *
    * @return true if the file should be reread.
    */
    boolean fileNeedsRereading() {
        Long modificationTimeStageConfigFile;
        modificationTimeStageConfigFile = _stageConfigFile.lastModified();

        return modificationTimeStageConfigFile > _lastTimeReadingStageConfigFile;
    }

    /**
    * Check whether the user matches predicates, that is, whether the user is in the
    * list of authorized users that are allowed to perform staging.
    *
    * @param dn user's Distinguished Name
    * @param fqan user's FQAN as a String
    * @return true if the user matches predicates.
    */
    boolean userMatchesPredicates(String dn, String fqanStr) {
        try {
            _fileReadLock.lock();
            for (Pattern[] regexLine : _regexList) {
                if ( regexLine[0].matcher(dn).matches() ) {

                    if ( regexLine[1] == null ) {
                        return true; // DN match and FQAN does not exist -> STAGE allowed
                    } else if ( regexLine[1].matcher(fqanStr).matches() ) {
                        return true; // DN and FQAN match -> STAGE allowed
                    }
                }
            }
        } finally {
            _fileReadLock.unlock();
        }
        return false;
    }

    /**
    * Read configuration file and create list of compiled patterns, containing DNs and FQANs
    * of the users that are allowed to perform staging.
    *
    * @param  reader
    * @return list of compiled patterns
    * @throws IOException
    * @throws PatternSyntaxException
    */
    List<Pattern[]> readStageConfigFile(BufferedReader reader) throws IOException, PatternSyntaxException {

        String line = null;
        Pattern linePattern = Pattern.compile("\"([^\"]*)\"([ \t]+\"([^\"]*)\")?");
        Matcher matcherLine;

        List<Pattern[]> regexList = new ArrayList<Pattern[]>();

        while ((line = reader.readLine()) != null) {

            line = line.trim();
            matcherLine = linePattern.matcher(line);
            if ( line.startsWith("#") || line.isEmpty() ) { //commented or empty line
                continue;
            }

            if ( !matcherLine.matches() )
                continue;

            Pattern[] arrayPattern = new Pattern[2];

            String matchDN = matcherLine.group(1);
            String matchFQAN = matcherLine.group(3);

            if ( matchFQAN != null ) {
                arrayPattern[0] = Pattern.compile(matchDN);
                arrayPattern[1] = Pattern.compile(matchFQAN);
                regexList.add(arrayPattern);
            } else {
                arrayPattern[0] = Pattern.compile(matcherLine.group(1));
                arrayPattern[1] = null;
                regexList.add(arrayPattern);
            }
        }
        return regexList;
    }

}
