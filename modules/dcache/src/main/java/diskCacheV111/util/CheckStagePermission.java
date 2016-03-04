package diskCacheV111.util;

import javax.security.auth.Subject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dcache.auth.FQAN;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;

public class CheckStagePermission {
    private File _stageConfigFile;
    private long _lastTimeReadingStageConfigFile;
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
     * Check whether staging is allowed for a particular subject on a particular object.
     *
     * @param subject The subject
     * @param fileAttributes The attributes of the file
     * @return true if and only if the subject is allowed to perform
     * staging
     */
    public boolean canPerformStaging(Subject subject, FileAttributes fileAttributes)
        throws PatternSyntaxException, IOException
    {
        if (!_isEnabled || Subjects.isRoot(subject)) {
            return true;
        }

        try {
            String dn = Subjects.getDn(subject);
            Collection<FQAN> fqans = Subjects.getFqans(subject);

            String storageClass = fileAttributes.getStorageClass();
            String hsm = fileAttributes.getHsm();

            String storeUnit = "";
            if (storageClass != null && hsm != null) {
                storeUnit = storageClass+"@"+hsm;
            }

            if (dn == null) {
                dn = "";
            }

            if (fqans.isEmpty()) {
                return canPerformStaging(dn, null, storeUnit);
            } else {
                for (FQAN fqan: fqans) {
                    if (canPerformStaging(dn, fqan, storeUnit)) {
                        return true;
                    }
                }
                return false;
            }
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("Subject has multiple DNs");
        }
    }

    /**
     * Check whether staging is allowed for the user with given DN and FQAN
     * for the object in the given storage group.
     *
     * @param  dn user's Distinguished Name
     * @param  fqan user's Fully Qualified Attribute Name
     * @param  storeUnit object's store unit
     * @return true if the user is allowed to perform staging of the object
     * @throws PatternSyntaxException
     * @throws IOException
     */
     public boolean canPerformStaging(String dn, FQAN fqan, String storeUnit) throws PatternSyntaxException, IOException {

         if ( !_isEnabled ) {
             return true;
         }

         if ( !_stageConfigFile.exists() ) {
             //if file does not exist, staging is denied for all users
             return false;
         }

         if ( fileNeedsRereading() ) {
             rereadConfig();
         }

         return userMatchesPredicates(dn, Objects.toString(fqan, ""), storeUnit);
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
                  try (BufferedReader reader = new BufferedReader(new FileReader(_stageConfigFile))) {
                      _regexList = readStageConfigFile(reader);
                      _lastTimeReadingStageConfigFile = System
                              .currentTimeMillis();
                  }

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
           long modificationTimeStageConfigFile;
           modificationTimeStageConfigFile = _stageConfigFile.lastModified();

           return modificationTimeStageConfigFile > _lastTimeReadingStageConfigFile;
       }

      /**
       * Check whether the user matches predicates, that is, whether the user is in the
       * list of authorized users that are allowed to perform staging of the object in the
       * given storage group.
       *
       * @param dn user's Distinguished Name
       * @param fqanStr user's FQAN as a String
       * @param storeUnit object's storage unit
       * @return true if the user and object match predicates
       */
       boolean userMatchesPredicates(String dn, String fqanStr, String storeUnit) {
           try {
               _fileReadLock.lock();
               for (Pattern[] regexLine : _regexList) {
                   if ( regexLine[0].matcher(dn).matches() ) {

                       if ( regexLine[1] == null ) {
                           return true; // line contains only DN; DN match -> STAGE allowed
                       } else if ( regexLine[1].matcher(fqanStr).matches() && (regexLine[2] == null || regexLine[2].matcher(storeUnit).matches()) ) {
                           return true;
                           //two cases covered here:
                           //line contains DN and FQAN; DN and FQAN match -> STAGE allowed
                           //line contains DN, FQAN, storeUnit; DN, FQAN, storeUnit match -> STAGE allowed
                       }
                   }
               }
           } finally {
               _fileReadLock.unlock();
           }
           return false;
       }

       /**
        * Read configuration file and create list of compiled patterns, containing DNs and FQANs(optionally)
        * of the users that are allowed to perform staging,
        * as well as storage group of the object to be staged (optionally).
        *
        * @param  reader
        * @return list of compiled patterns
        * @throws IOException
        * @throws PatternSyntaxException
        */
        List<Pattern[]> readStageConfigFile(BufferedReader reader) throws IOException, PatternSyntaxException {

            String line;
            Pattern linePattern = Pattern.compile("\"([^\"]*)\"([ \t]+\"([^\"]*)\"([ \t]+\"([^\"]*)\")?)?");
            Matcher matcherLine;

            List<Pattern[]> regexList = new ArrayList<>();

            while ((line = reader.readLine()) != null) {

                line = line.trim();
                matcherLine = linePattern.matcher(line);
                if ( line.startsWith("#") || line.isEmpty() ) { //commented or empty line
                    continue;
                }

                if ( !matcherLine.matches() ) {
                    continue;
                }

                Pattern[] arrayPattern = new Pattern[3];

                String matchDN = matcherLine.group(1);
                String matchFQAN = matcherLine.group(3);
                String matchStoreUnit = matcherLine.group(5);

                if ( matchFQAN != null ) {
                    if (matchStoreUnit != null) { //line: DN, FQAN, StoreUnit
                        arrayPattern[0] = Pattern.compile(matchDN);
                        arrayPattern[1] = Pattern.compile(matchFQAN);
                        arrayPattern[2] = Pattern.compile(matchStoreUnit);
                        regexList.add(arrayPattern);
                    } else { //line: DN, FQAN
                        arrayPattern[0] = Pattern.compile(matchDN);
                        arrayPattern[1] = Pattern.compile(matchFQAN);
                        arrayPattern[2] = null;
                        regexList.add(arrayPattern);
                    }
                } else { //line: DN
                    arrayPattern[0] = Pattern.compile(matchDN);
                    arrayPattern[1] = null;
                    arrayPattern[2] = null;
                    regexList.add(arrayPattern);
                }
            }
            return regexList;
        }

}
