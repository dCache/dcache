package org.dcache.acl.handler;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.acl.config.AclConfig;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;

/**
 * This component comprises of file system context dependent methods, e.g. an ACL inheritance handler.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class AclFsHandler extends DefaultACLHandler {

    private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + "logger.org.dcache.authorization." + AclFsHandler.class.getName());

    public AclFsHandler() throws ACLException {
        super();
    }

    public AclFsHandler(String configFile) throws ACLException {
        super(configFile);
    }

    public AclFsHandler(AclConfig aclConfig) throws ACLException {
        super(aclConfig);
    }

    /**
     * Sets the ACL to the new child resource
     *
     * @param parentACL
     *            ACL of parent resource
     * @param childID
     *            Child resource ID
     * @param isChildDir
     *            <code>FALSE</code> if child resource is non-directory file, otherwise <code>TRUE</code>
     *
     * @return <code>TRUE</code> if operation succeed, otherwise <code>FALSE</code>
     * @throws ACLException
     */
    public boolean inheritACL(final ACL parentACL, final String childID, boolean isChildDir) throws ACLException {
        List<ACE> aces = getDefaultACEs(parentACL, isChildDir);
        return (aces.size() == 0 ? true : setACL(new ACL(childID, (isChildDir ? RsType.DIR : RsType.FILE), aces)));
    }

    /**
     * Retrieve the default list of ACE from parentACL
     *
     * @param parentACL
     *            ACL of parent resource
     * @param isChildDir
     *            <code>FALSE</code> if child resource is non-directory file, otherwise <code>TRUE</code>
     * @return <code>TRUE</code> if operation succeed, otherwise <code>FALSE</code>
     * @throws ACLException
     */
    public List<ACE> getDefaultACEs(final ACL parentACL, boolean isChildDir) throws ACLException {
        try {
            List<ACE> aces = new ArrayList<ACE>();

            final String parentID = parentACL.getRsId();
            long startTime = 0;
            if (logger.isDebugEnabled()) {
                logger.debug("Get default ACEs for " + (isChildDir ? "directory" : "file") + " from parent: " + parentID);
                startTime = System.currentTimeMillis();
            }

            int order = 0, parentFlags = 0;

            List<ACE> parentAces = parentACL.getList();
            for (ACE ace : parentAces) {
                boolean addACE = false;
                parentFlags = ace.getFlags();
                int flags = 0;
                if (isChildDir) { // child is DIRECTORY ***********************************
                    if (AceFlags.FILE_INHERIT_ACE.matches(parentFlags))
                        flags = AceFlags.FILE_INHERIT_ACE.getValue();

                    if (AceFlags.DIRECTORY_INHERIT_ACE.matches(parentFlags))
                        flags |= AceFlags.DIRECTORY_INHERIT_ACE.getValue();

                    if (addACE = (flags != 0)) {
                        if (AceFlags.IDENTIFIER_GROUP.matches(parentFlags))
                            flags |= AceFlags.IDENTIFIER_GROUP.getValue();

                    } else if (AceFlags.INHERIT_ONLY_ACE.matches(parentFlags))
                        logger.warn("Unsupported AceFlags.INHERIT_ONLY_ACE flag in ace: ace.order = " + ace.getOrder() + ", ace.rsID = " + parentID);

                } else { // child is non-directory FILE ***********************************
                    if (addACE = AceFlags.FILE_INHERIT_ACE.matches(parentFlags)) {
                        if (AceFlags.IDENTIFIER_GROUP.matches(parentFlags))
                            flags |= AceFlags.IDENTIFIER_GROUP.getValue();

                    } else if (AceFlags.INHERIT_ONLY_ACE.matches(parentFlags) && AceFlags.DIRECTORY_INHERIT_ACE.matches(parentFlags) == false)
                        logger.warn("Unsupported AceFlags.INHERIT_ONLY_ACE flag in ace: ace.order = " + ace.getOrder() + ", ace.rsID = " + parentID);
                }

                if (addACE) {
                    logger.debug("AclFsHandler::getDefaultACEs in if(addACE), i.e. ACE will be added ");
                    aces.add(new ACE(ace.getType(), flags, ace.getAccessMsk(), ace.getWho(), ace.getWhoID(), ace.getAddressMsk(), order++));
                }
            }

            if (logger.isDebugEnabled())
                logger.debug("TIMING: Get default ACEs in " + (System.currentTimeMillis() - startTime) + " msec");

            return aces;

        } catch (Exception e) {
            throw new ACLException("Get default ACEs", e);
        }
    }

}
