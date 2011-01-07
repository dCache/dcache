package org.dcache.gplazma.plugins;

import org.glite.authz.common.model.Action;
import org.glite.authz.common.model.Attribute;
import org.glite.authz.common.model.Environment;
import org.glite.authz.common.model.Request;
import org.glite.authz.common.model.Resource;
import org.glite.authz.common.model.Subject;
import org.glite.authz.pep.profile.AuthorizationProfile;

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
        actionAttrib.setId(AuthorizationProfile.ID_ATTRIBUTE_ACTION_ID);
        actionAttrib.setDataType(AuthorizationProfile.DATATYPE_STRING);
        actionAttrib.setIssuer(null);
        actionAttrib.getValues().add(actionId);
        action.getAttributes().add(actionAttrib);

        Resource resource = new Resource();

        Attribute resourceAttrib = new Attribute();
        resourceAttrib.setId(AuthorizationProfile.ID_ATTRIBUTE_RESOURCE_ID);
        resourceAttrib.setDataType(AuthorizationProfile.DATATYPE_STRING);
        resourceAttrib.setIssuer(null);
        resourceAttrib.getValues().add(resourceId);
        resource.getAttributes().add(resourceAttrib);

        Environment environment = new Environment();
        Attribute environmentAttrib = new Attribute();
        environmentAttrib.setId(AuthorizationProfile.ID_ATTRIBUTE_PROFILE_ID);
        environmentAttrib.setDataType(AuthorizationProfile.DATATYPE_STRING);
        environmentAttrib.setIssuer(null);
        environmentAttrib.getValues().add(profile.getProfileId());
        environment.getAttributes().add(environmentAttrib);

        Subject subject = new Subject();
        subject.setCategory(null);

        Attribute subjectAttribute = new Attribute();
        subjectAttribute.setId(AuthorizationProfile.ID_ATTRIBUTE_SUBJECT_ID);
        subjectAttribute.setDataType("urn:oasis:names:tc:xacml:1.0:data-type:x500Name");
        subjectAttribute.setIssuer(null);
        subjectAttribute.getValues().add(dn);
        subject.getAttributes().add(subjectAttribute);

        Request request = profile.createRequest(subject, resource, action, environment);
        return request;
    }
}
