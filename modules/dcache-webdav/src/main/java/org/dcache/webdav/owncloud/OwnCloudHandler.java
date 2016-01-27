/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav.owncloud;

import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.StringUtil;
import org.codehaus.jackson.map.*;
import org.springframework.http.MediaType;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Class mapping certain paths to confront to the requirements of the OwnCloud Sync client for finding out server status and capabilities.
 *
 * @author ugrin
 */
public class OwnCloudHandler extends RewriteHandler
{
    private static final String OWNCLOUD_STATUS_ENDPOINT = "/status.php";
    private static final String OWNCLOUD_CAPABILITIES_ENDPOINT = "/ocs/v1.php/cloud/capabilities";

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
    {
        if (OwncloudClients.isSyncClient(request) && baseRequest.getMethod().equals(HttpMethod.GET.toString())) {

            String json = null;

            switch (target)
            {
                case OWNCLOUD_STATUS_ENDPOINT:
                json = buildStatusResponse();
                break;

                case OWNCLOUD_CAPABILITIES_ENDPOINT:
                json = buildCapabilitiesResponse();
                break;

            }

            if (json != null) {
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                response.setCharacterEncoding(StringUtil.__UTF8);
                response.getOutputStream().write(json.getBytes());
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
            }
        }
    }

    String buildStatusResponse() throws IOException
    {
        Map<String, Object> paramsMap = new HashMap<>();

        paramsMap.put("edition", "");
        paramsMap.put("installed", true);
        paramsMap.put("maintenance", false);
        paramsMap.put("version", "8.2.0.12");
        paramsMap.put("versionstring", "8.2.0");

        return new ObjectMapper().writeValueAsString(paramsMap);
    }

    String buildCapabilitiesResponse() throws IOException
    {
        Map<String, Object> paramsMap = new HashMap<>();

        Map<String, Object> metaMap = new HashMap<>();
        metaMap.put("status", "ok");
        metaMap.put("statuscode", 100);
        metaMap.put("message", null);
        paramsMap.put("meta", metaMap);

        Map<String, Object> dataMap = new HashMap<>();

        Map<String, Object> versionMap = new HashMap<>();
        versionMap.put("major", 8);
        versionMap.put("minor", 2);
        versionMap.put("micro", 0);
        versionMap.put("string", "8.2.0");
        versionMap.put("edition", "");
        dataMap.put("version", versionMap);

        Map<String, Object> capabilitiesMap = new HashMap<>();

        Map<String, Object> coreMap = new HashMap<>();
        coreMap.put("pollinterval", 60);
        capabilitiesMap.put("core", coreMap);

        Map<String, Object> fileSharingMap = new HashMap<>();
        fileSharingMap.put("api_enabled", true);

        Map<String, Object> publicMap = new HashMap<>();
        publicMap.put("enabled", true);
        Map<String, Object> enforcedPasswordMap = new HashMap<>();
        enforcedPasswordMap.put("enforced", false);
        publicMap.put("password", enforcedPasswordMap);
        Map<String, Object> expiredateMap = new HashMap<>();
        expiredateMap.put("enabled", false);
        publicMap.put("expire_date", expiredateMap);
        publicMap.put("send_mail", false);
        publicMap.put("upload", false);
        fileSharingMap.put("public", publicMap);

        Map<String, Object> sendMailMap = new HashMap<>();
        sendMailMap.put("send_mail", false);
        fileSharingMap.put("user", sendMailMap);
        fileSharingMap.put("resharing", true);

        Map<String, Object> federationMap = new HashMap<>();
        federationMap.put("outgoing", true);
        federationMap.put("incoming", true);
        fileSharingMap.put("federation", federationMap);

        capabilitiesMap.put("files_sharing", fileSharingMap);

        Map<String, Object> filesMap = new HashMap<>();
        filesMap.put("bigfilechunking", false);
        filesMap.put("undelete", false);
        filesMap.put("versioning", false);
        capabilitiesMap.put("files", filesMap);

        ArrayList<String> endpoints = new ArrayList<>();
        Map<String, Object> endpointsMap = new HashMap<>();
        endpointsMap.put("endpoints", endpoints.toArray());
        capabilitiesMap.put("notifications", endpointsMap);

        dataMap.put("capabilities", capabilitiesMap);
        paramsMap.put("data", dataMap);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("ocs", paramsMap);

        return new ObjectMapper().writeValueAsString(rootMap);
    }
}
