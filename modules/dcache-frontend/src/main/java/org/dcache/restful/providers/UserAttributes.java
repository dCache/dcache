/*
 * dCache - http://www.dcache.org/
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
package org.dcache.restful.providers;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;
import java.util.Objects;

/**
 * Class to hold information for a JSON response querying information
 * about a user.
 */
@ApiModel(description = "Description about a specific user.")
public class UserAttributes {
    public enum AuthenticationStatus {ANONYMOUS, AUTHENTICATED}

    /**
     * Whether the current user has authenticated with the system.
     * ANONYMOUS indicates that the user supplied no credentials or that
     * the credentials failed to authenticate the user (e.g., wrong password).
     */
    @ApiModelProperty(value = "The authentication status of this user.",
                    allowableValues = "ANONYMOUS, AUTHENTICATED")
    private AuthenticationStatus status;

    /**
     * The UID of the user, if the user has status AUTHENTICATED, null
     * otherwise.
     */
    @ApiModelProperty(value = "The numerical uid for this user.",
                    allowableValues = "range[0,infinity]")
    private Long uid;

    @ApiModelProperty(value = "The numerical gids for this user, the first "
                    + "value is the primary gid.",
                    allowableValues = "range[0,infinity]")
    private List<Long> gids;

    @ApiModelProperty("The list of roles that the user choose to assert.")
    private List<String> roles;

    @ApiModelProperty("The list of roles that the user is entitled to "
                    + "assert, but chose not to.")
    private List<String> unassertedRoles;

    @ApiModelProperty(value = "The user's home directory.")
    private String home;

    @ApiModelProperty(value = "The user's root directory.")
    private String root;

    @ApiModelProperty(value = "The username for this user.")
    private String username;

    @ApiModelProperty("The list of email addresses known for this user.")
    private List<String> email;

    public List<String> getEmail() {
        return email;
    }

    public List<Long> getGids() {
        return gids;
    }

    public String getHomeDirectory() {
        return home;
    }

    public List<String> getRoles() {
        return roles;
    }

    public String getRootDirectory() {
        return root;
    }

    public AuthenticationStatus getStatus() {
        return status;
    }

    public Long getUid() {
        return uid;
    }

    public List<String> getUnassertedRoles() {
        return unassertedRoles;
    }

    public String getUsername() {
        return username;
    }

    public void setEmail(List<String> email) {
        this.email = email;
    }

    public void setGids(List<Long> gids) {
        this.gids = gids;
    }

    public void setHomeDirectory(String dir) {
        home = dir;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public void setRootDirectory(String dir) {
        root = dir;
    }

    public void setStatus(AuthenticationStatus status) {
        this.status = Objects.requireNonNull(status);
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public void setUnassertedRoles(List<String> roles) {
        this.unassertedRoles = roles;
    }

    public void setUsername(String name) {
        username = name;
    }
}