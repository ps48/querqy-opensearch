/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package querqy.opensearch.security;

import org.opensearch.commons.authuser.User;
import org.opensearch.rest.RestStatus;
import querqy.opensearch.settings.PluginSettings;
import querqy.opensearch.settings.PluginSettings.FilterBy;
import org.opensearch.OpenSearchStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for checking/filtering user access.
 */
public class UserAccessManager {
    private static final String USER_TAG = "User:";
    private static final String ROLE_TAG = "Role:";
    private static final String BACKEND_ROLE_TAG = "BERole:";
    private static final String ALL_ACCESS_ROLE = "all_access";
    private static final String OPENSEARCH_DASHBOARDS_SERVER_USER = "opensearchdashboardsserver"; // TODO: Change it to background user when created.
    private static final String PRIVATE_TENANT = "__user__";
    public static final String DEFAULT_TENANT = "";
    private static final PluginSettings pluginSettings = PluginSettings.getInstance();

//    PluginSettings PluginSettings = new PluginSettings();

    //    /**
//     * Validate User if eligible to do operation
//     * If filterBy == NoFilter
//     * -> No validation
//     * If filterBy == User
//     * -> User name should be present
//     * If filterBy == Roles
//     * -> roles should be present
//     * If filterBy == BackendRoles
//     * -> backend roles should be present
//     */
    public static void validateUser(User user) {
        if (isUserPrivateTenant(user) && user.getName() == null) {
            throw new OpenSearchStatusException(
                    "User name not provided for private tenant access",
                    RestStatus.FORBIDDEN
            );
        }

        if (pluginSettings.filterBy == FilterBy.User) {
            if (user.getName() == null) {
                throw new OpenSearchStatusException("Filter-by enabled with security disabled",
                        RestStatus.FORBIDDEN);
            }
        }

        if (pluginSettings.filterBy == FilterBy.Roles) {
            if (user == null || user.getRoles().isEmpty()) {
                throw new OpenSearchStatusException("No distinguishing roles configured. Contact administrator.",
                        RestStatus.FORBIDDEN);
            }
        }

        if (pluginSettings.filterBy == FilterBy.BackendRoles) {
            if (user == null || user.getBackendRoles().isEmpty()) {
                throw new OpenSearchStatusException("User doesn't have backend roles configured. Contact administrator.",
                        RestStatus.FORBIDDEN);
            }
        }


    }

    /**
     * validate if user has access to polling actions
     */
    public void validatePollingUser(User user) {
        if (user != null) { // Check only if security is enabled
            if (!user.getName().equals(OPENSEARCH_DASHBOARDS_SERVER_USER)) {
                throw new OpenSearchStatusException("Permission denied", RestStatus.FORBIDDEN);
            }
        }
    }


    /**
     * Get tenant info from user object.
     */
    public static String getUserTenant(User user) {
        if (user == null || user.getRequestedTenant() == null) {
            return DEFAULT_TENANT;
        } else {
            return user.getRequestedTenant();
        }
    }

    /**
     * Get all user access info from user object.
     */
    public static List<String> getAllAccessInfo(User user) {
        if (user == null) { // Security is disabled
            return new ArrayList<>();
        }
        ArrayList<String> retList = new ArrayList<>();
        if (user.getName() != null) {
            retList.add(USER_TAG + user.getName());
        }
        user.getRoles().forEach(it -> retList.add(ROLE_TAG + it));
        user.getBackendRoles().forEach(it -> retList.add(BACKEND_ROLE_TAG + it));
        return retList;
    }

    /**
     * Get access info for search filtering
     */
    public List<String> getSearchAccessInfo(User user) {
        if (user == null) { // Security is disabled
            return new ArrayList<>();
        }
        if (isUserPrivateTenant(user)) {
            ArrayList<String> users = new ArrayList<>();
            users.add(USER_TAG+user.getName());
            return users; // No sharing allowed in private tenant.
        }
        if (canAdminViewAllItems(user)) {
            return new ArrayList<>();
        }
        if (pluginSettings.filterBy == FilterBy.NoFilter) return new ArrayList<>();
        if (pluginSettings.filterBy == FilterBy.User) {
            ArrayList<String> users = new ArrayList<>();
            users.add(USER_TAG+user.getName());
            return users; // No sharing allowed in private tenant.
        }
        if (pluginSettings.filterBy == FilterBy.Roles) {
            return user.getRoles().stream().filter(it -> !pluginSettings.ignoredRoles.contains(it)).map(it -> ROLE_TAG + it).collect(Collectors.toList());
        }
        if (pluginSettings.filterBy == FilterBy.BackendRoles) {
            return user.getBackendRoles().stream().map(it -> BACKEND_ROLE_TAG + it).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    /**
     * validate if user has access based on given access list
     */
    public static Boolean doesUserHasAccess(User user, String tenant, List<String> access) {
        if (user == null) { // Security is disabled
            return true;
        }
        if (!getUserTenant(user).equals(tenant)) {
            return false;
        }
        if (canAdminViewAllItems(user)) {
            return true;
        }

        if (pluginSettings.filterBy == FilterBy.NoFilter) return true;
        if (pluginSettings.filterBy == FilterBy.User) return access.contains(USER_TAG+user.getName());
        if (pluginSettings.filterBy == FilterBy.Roles)
            return user.getRoles().stream().filter(it -> !pluginSettings.ignoredRoles.contains(it)).map(it -> ROLE_TAG + it).anyMatch(access::contains);
        if (pluginSettings.filterBy == FilterBy.BackendRoles)
            return user.getBackendRoles().stream().map(it -> BACKEND_ROLE_TAG + it).anyMatch(access::contains);
        return false;
    }

    /**
     * Check if user has all info access.
     */
    public Boolean hasAllInfoAccess(User user) {
        if (user == null) { // Security is disabled
            return true;
        }
        return isAdminUser(user);
    }

    private static Boolean canAdminViewAllItems(User user) {
        return pluginSettings.adminAccess == PluginSettings.AdminAccess.AllObservabilityObjects && isAdminUser(user);
    }

    private static Boolean isAdminUser(User user) {
        return user.getRoles().contains(ALL_ACCESS_ROLE);
    }

    private static Boolean isUserPrivateTenant(User user) {
        return getUserTenant(user).equals(PRIVATE_TENANT);
    }
}
