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
@ApiModel(value="User information.",
        description="Description about a specific user.")
public class UserAttributes
{
    public enum AuthenticationStatus {ANONYMOUS, AUTHENTICATED};

    /**
     * Whether the current user has authenticated with the system.
     * ANONYMOUS indicates that the user supplied no credentials or that
     * the credentials failed to authenticate the user (e.g., wrong password).
     *
     */
    private AuthenticationStatus status;

    /**
     * The UID of the user, if the user has status AUTHENTICATED, null
     * otherwise.
     */
    private Long uid;

    private List<Long> gids;

    private List<String> roles;

    private List<String> unassertedRoles;

    private String home;

    private String root;

    private String username;

    private List<String> email;

    @ApiModelProperty(value = "The authentication status of this user.",
            allowableValues = "ANONYMOUS,AUTHENTICATED")
    public AuthenticationStatus getStatus()
    {
        return status;
    }

    public void setStatus(AuthenticationStatus status)
    {
        this.status = Objects.requireNonNull(status);
    }

    @ApiModelProperty(value = "The numerical uid for this user.",
            allowableValues = "range[0,infinity]")
    public Long getUid()
    {
        return uid;
    }

    public void setUid(Long uid)
    {
        this.uid = uid;
    }

    @ApiModelProperty(value = "The numerical gids for this user, the first "
                    + "value is the primary gid.",
            allowableValues = "range[0,infinity]")
    public List<Long> getGids()
    {
        return gids;
    }

    public void setGids(List<Long> gids)
    {
        this.gids = gids;
    }

    @ApiModelProperty(value = "The user's home directory.")
    public String getHomeDirectory()
    {
        return home;
    }

    public void setHomeDirectory(String dir)
    {
        home = dir;
    }

    @ApiModelProperty(value = "The user's root directory.")
    public String getRootDirectory()
    {
        return root;
    }

    public void setRootDirectory(String dir)
    {
        root = dir;
    }

    @ApiModelProperty(value = "The username for this user.")
    public String getUsername()
    {
        return username;
    }

    public void setUsername(String name)
    {
        username = name;
    }

    @ApiModelProperty("The list of roles that the user choose to assert.")
    public List<String> getRoles()
    {
        return roles;
    }

    public void setRoles(List<String> roles)
    {
        this.roles = roles;
    }

    @ApiModelProperty("The list of roles that the user is entitled to "
            + "assert, but chose not to.")
    public List<String> getUnassertedRoles()
    {
        return unassertedRoles;
    }

    public void setUnassertedRoles(List<String> roles)
    {
        this.unassertedRoles = roles;
    }

    @ApiModelProperty("The list of email addresses known for this user.")
    public List<String> getEmail()
    {
        return email;
    }

    public void setEmail(List<String> email)
    {
        this.email = email;
    }
}