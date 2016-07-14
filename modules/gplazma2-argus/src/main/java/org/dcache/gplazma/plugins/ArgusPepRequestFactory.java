package org.dcache.gplazma.plugins;

import org.glite.authz.common.model.Action;
import org.glite.authz.common.model.Attribute;
import org.glite.authz.common.model.Environment;
import org.glite.authz.common.model.Request;
import org.glite.authz.common.model.Resource;
import org.glite.authz.common.model.Subject;
import org.glite.authz.pep.profile.AuthorizationProfile;

import static org.glite.authz.common.profile.CommonXACMLAuthorizationProfileConstants.*;

public class ArgusPepRequestFactory {

    /**
     * @param dn DN of the subject
     * @param resourceId id of the resource to be accessed
     * @param actionId id of the action to be performed on the resource
     * @param profile id of the access profile (i.e. WN/CE)
     * @return a glite authz request to be sent to a PEPd
     */
    public static Request create(String dn, String resourceId, String actionId, AuthorizationProfile profile) {

        Action action = new Action();

        Attribute actionAttrib = new Attribute();
        actionAttrib.setId(ID_ATTRIBUTE_ACTION_ID);
        actionAttrib.setDataType(DATATYPE_STRING);
        actionAttrib.setIssuer(null);
        actionAttrib.getValues().add(actionId);
        action.getAttributes().add(actionAttrib);

        Resource resource = new Resource();

        Attribute resourceAttrib = new Attribute();
        resourceAttrib.setId(ID_ATTRIBUTE_RESOURCE_ID);
        resourceAttrib.setDataType(DATATYPE_STRING);
        resourceAttrib.setIssuer(null);
        resourceAttrib.getValues().add(resourceId);
        resource.getAttributes().add(resourceAttrib);

        Environment environment = new Environment();
        Attribute environmentAttrib = new Attribute();
        environmentAttrib.setId(ID_ATTRIBUTE_PROFILE_ID);
        environmentAttrib.setDataType(DATATYPE_STRING);
        environmentAttrib.setIssuer(null);
        environmentAttrib.getValues().add(profile.getProfileId());
        environment.getAttributes().add(environmentAttrib);

        Subject subject = new Subject();
        subject.setCategory(null);

        Attribute subjectAttribute = new Attribute();
        subjectAttribute.setId(ID_ATTRIBUTE_SUBJECT_ID);
        subjectAttribute.setDataType(DATATYPE_STRING);
        subjectAttribute.setIssuer(null);
        subjectAttribute.getValues().add(dn);
        subject.getAttributes().add(subjectAttribute);

        Request request = profile.createRequest(subject, resource, action, environment);
        return request;
    }
}
