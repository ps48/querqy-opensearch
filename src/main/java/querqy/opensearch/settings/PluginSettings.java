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

package querqy.opensearch.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.bootstrap.BootstrapInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

public class PluginSettings {

    String LOG_PREFIX = "opensearch-querqy";

    /**
     * Settings Key prefix for this plugin.
     */
    private String KEY_PREFIX = "opensearch.querqy";

    /**
     * General settings Key prefix.
     */
    private String GENERAL_KEY_PREFIX = "$KEY_PREFIX.general";

    /**
     * Polling settings Key prefix.
     */
    private String POLLING_KEY_PREFIX = "$KEY_PREFIX.polling";

    /**
     * Access settings Key prefix.
     */
    private String ACCESS_KEY_PREFIX = "$KEY_PREFIX.access";

    /**
     * Operation timeout for network operations.
     */
    private String OPERATION_TIMEOUT_MS_KEY = "$GENERAL_KEY_PREFIX.operationTimeoutMs";

    /**
     * Setting to choose Job lock duration.
     */
    private String JOB_LOCK_DURATION_S_KEY = "$POLLING_KEY_PREFIX.jobLockDurationSeconds";

    /**
     * Setting to choose Minimum polling duration.
     */
    private String MIN_POLLING_DURATION_S_KEY = "$POLLING_KEY_PREFIX.minPollingDurationSeconds";

    /**
     * Setting to choose Maximum polling duration.
     */
    private String MAX_POLLING_DURATION_S_KEY = "$POLLING_KEY_PREFIX.maxPollingDurationSeconds";

    /**
     * Setting to choose Maximum number of retries to try locking.
     */
    private String MAX_LOCK_RETRIES_KEY = "$POLLING_KEY_PREFIX.maxLockRetries";

    /**
     * Setting to choose default number of items to query.
     */
    private String DEFAULT_ITEMS_QUERY_COUNT_KEY = "$GENERAL_KEY_PREFIX.defaultItemsQueryCount";

    /**
     * Setting to choose admin access restriction.
     */
    private String ADMIN_ACCESS_KEY = "$ACCESS_KEY_PREFIX.adminAccess";

    /**
     * Setting to choose filter method.
     */
    private String FILTER_BY_KEY = "$ACCESS_KEY_PREFIX.filterBy";

    /**
     * Setting to choose ignored roles for filtering.
     */
    private String IGNORE_ROLE_KEY = "$ACCESS_KEY_PREFIX.ignoreRoles";

    /**
     * Default operation timeout for network operations.
     */
    private Long DEFAULT_OPERATION_TIMEOUT_MS = 60000L;

    /**
     * Minimum operation timeout for network operations.
     */
    private Long MINIMUM_OPERATION_TIMEOUT_MS = 100L;

    /**
     * Default Job lock duration.
     */
    private Integer DEFAULT_JOB_LOCK_DURATION_S = 300;

    /**
     * Minimum Job lock duration.
     */
    private Integer MINIMUM_JOB_LOCK_DURATION_S = 10;

    /**
     * Default Minimum polling duration.
     */
    private Integer DEFAULT_MIN_POLLING_DURATION_S = 300;

    /**
     * Minimum Min polling duration.
     */
    private Integer MINIMUM_MIN_POLLING_DURATION_S = 60;

    /**
     * Default Maximum polling duration.
     */
    private Integer DEFAULT_MAX_POLLING_DURATION_S = 900;

    /**
     * Minimum Maximum polling duration.
     */
    private Integer MINIMUM_MAX_POLLING_DURATION_S = 300;

    /**
     * Default number of retries to try locking.
     */
    private Integer DEFAULT_MAX_LOCK_RETRIES = 4;

    /**
     * Minimum number of retries to try locking.
     */
    private Integer MINIMUM_LOCK_RETRIES = 1;

    /**
     * Default number of items to query.
     */
    private Integer DEFAULT_ITEMS_QUERY_COUNT_VALUE = 1000;

    /**
     * Minimum number of items to query.
     */
    private Integer MINIMUM_ITEMS_QUERY_COUNT = 10;

    /**
     * Default admin access method.
     */
    private String DEFAULT_ADMIN_ACCESS_METHOD = "AllObservabilityObjects";

    /**
     * Default filter-by method.
     */
    private String DEFAULT_FILTER_BY_METHOD = "NoFilter";

    /**
     * Default filter-by method.
     */
    private ArrayList<String> DEFAULT_IGNORED_ROLES = new ArrayList<>(
            Arrays.asList(
                    "own_index",
                    "opensearch_dashboards_user",
                    "notebooks_full_access",
                    "notebooks_read_access"
            ));

    /**
     * Operation timeout setting in ms for I/O operations
     */
    public static volatile Long operationTimeoutMs;


    /**
     * Job lock duration
     */
    public static volatile Integer jobLockDurationSeconds;

    /**
     * Minimum polling duration
     */
    public static volatile Integer minPollingDurationSeconds;

    /**
     * Maximum polling duration.
     */
    public static volatile Integer maxPollingDurationSeconds;

    /**
     * Max number of retries to try locking.
     */
    public static volatile Integer maxLockRetries;

    /**
     * Default number of items to query.
     */
    public static volatile Integer defaultItemsQueryCount;

    /**
     * admin access method.
     */
    public static volatile AdminAccess adminAccess;

    /**
     * Filter-by method.
     */
    public static volatile FilterBy filterBy;

    /**
     * list of ignored roles.
     */
    public static volatile List<String> ignoredRoles;

    //    /**
//     * Enum for types of admin access
//     * "Standard" -> Admin user access follows standard user
//     * "AllObservabilityObjects" -> Admin user with "all_access" role can see all observability objects of all users.
//     */
    public enum AdminAccess {Standard, AllObservabilityObjects}

    //    /**
//     * Enum for types of filterBy options
//     * NoFilter -> everyone see each other's notebooks
//     * User -> querqy rules are visible to only themselves
//     * Roles -> querqy rules are visible to users having any one of the role of creator
//     * BackendRoles -> querqy rules are visible to users having any one of the backend role of creator
//     */
    public enum FilterBy {NoFilter, User, Roles, BackendRoles}

    private Integer DECIMAL_RADIX = 10;

    private static final Logger log = LogManager.getLogger(PluginSettings.class);
    private Map<String, String> defaultSettings;

    {
        Settings settings = null;
        String configDirName = BootstrapInfo.getSystemProperties().get("opensearch.path.conf").toString();
        if (configDirName != null) {
            Path defaultSettingYmlFile = Paths.get(configDirName, "opensearch-querqy", "querqy.yml");
            try {
                settings = Settings.builder().loadFromPath(defaultSettingYmlFile).build();
            } catch (IOException exception) {
                log.warn("$LOG_PREFIX:Failed to load ${defaultSettingYmlFile.toAbsolutePath()}");
            }
        }

        // Initialize the settings values to default values
        operationTimeoutMs = Optional.ofNullable(Long.parseLong(settings.get(OPERATION_TIMEOUT_MS_KEY))).orElse(DEFAULT_OPERATION_TIMEOUT_MS);
        jobLockDurationSeconds = Optional.ofNullable(Integer.parseInt(settings.get(JOB_LOCK_DURATION_S_KEY))).orElse(DEFAULT_JOB_LOCK_DURATION_S);
        minPollingDurationSeconds = Optional.ofNullable(Integer.parseInt(settings.get(MIN_POLLING_DURATION_S_KEY))).orElse(DEFAULT_MIN_POLLING_DURATION_S);
        maxPollingDurationSeconds = Optional.ofNullable(Integer.parseInt(settings.get(MAX_POLLING_DURATION_S_KEY))).orElse(DEFAULT_MAX_POLLING_DURATION_S);
        maxLockRetries = Optional.ofNullable(Integer.parseInt(settings.get(MAX_LOCK_RETRIES_KEY))).orElse(DEFAULT_MAX_LOCK_RETRIES);
        defaultItemsQueryCount = Optional.ofNullable(Integer.parseInt(settings.get(DEFAULT_ITEMS_QUERY_COUNT_KEY))).orElse(DEFAULT_ITEMS_QUERY_COUNT_VALUE);
        adminAccess = AdminAccess.valueOf(Optional.ofNullable(settings.get(ADMIN_ACCESS_KEY)).orElse(DEFAULT_ADMIN_ACCESS_METHOD));
        filterBy = FilterBy.valueOf(Optional.ofNullable(settings.get(FILTER_BY_KEY)).orElse(DEFAULT_FILTER_BY_METHOD));
        ignoredRoles = Optional.ofNullable(settings.getAsList(IGNORE_ROLE_KEY)).orElse(DEFAULT_IGNORED_ROLES);


        defaultSettings.put(OPERATION_TIMEOUT_MS_KEY, operationTimeoutMs.toString(DECIMAL_RADIX));
        defaultSettings.put(JOB_LOCK_DURATION_S_KEY, jobLockDurationSeconds.toString(DECIMAL_RADIX));
        defaultSettings.put(MIN_POLLING_DURATION_S_KEY, minPollingDurationSeconds.toString(DECIMAL_RADIX));
        defaultSettings.put(MAX_POLLING_DURATION_S_KEY, maxPollingDurationSeconds.toString(DECIMAL_RADIX));
        defaultSettings.put(MAX_LOCK_RETRIES_KEY, maxLockRetries.toString(DECIMAL_RADIX));
        defaultSettings.put(DEFAULT_ITEMS_QUERY_COUNT_KEY, defaultItemsQueryCount.toString(DECIMAL_RADIX));
        defaultSettings.put(ADMIN_ACCESS_KEY, adminAccess.name());
        defaultSettings.put(FILTER_BY_KEY, filterBy.name());


    }

    Setting<Long> OPERATION_TIMEOUT_MS = Setting.longSetting(
            OPERATION_TIMEOUT_MS_KEY,
            Long.parseLong(defaultSettings.get(OPERATION_TIMEOUT_MS_KEY)),
            MINIMUM_OPERATION_TIMEOUT_MS,
            NodeScope, Dynamic);

    Setting<Integer> JOB_LOCK_DURATION_S = Setting.intSetting(
            JOB_LOCK_DURATION_S_KEY,
            Integer.parseInt(defaultSettings.get(JOB_LOCK_DURATION_S_KEY)),
            MINIMUM_JOB_LOCK_DURATION_S,
            NodeScope, Dynamic
    );

    Setting<Integer> MIN_POLLING_DURATION_S = Setting.intSetting(
            MIN_POLLING_DURATION_S_KEY,
            Integer.parseInt(defaultSettings.get(MIN_POLLING_DURATION_S_KEY)),
            MINIMUM_MIN_POLLING_DURATION_S,
            NodeScope, Dynamic
    );

    Setting<Integer> MAX_POLLING_DURATION_S = Setting.intSetting(
            MAX_POLLING_DURATION_S_KEY,
            Integer.parseInt(defaultSettings.get(MAX_POLLING_DURATION_S_KEY)),
            MINIMUM_MAX_POLLING_DURATION_S,
            NodeScope, Dynamic
    );

    Setting<Integer> MAX_LOCK_RETRIES = Setting.intSetting(
            MAX_LOCK_RETRIES_KEY,
            Integer.parseInt(defaultSettings.get(MAX_LOCK_RETRIES_KEY)),
            MINIMUM_LOCK_RETRIES,
            NodeScope, Dynamic
    );

    Setting<Integer> DEFAULT_ITEMS_QUERY_COUNT = Setting.intSetting(
            DEFAULT_ITEMS_QUERY_COUNT_KEY,
            Integer.parseInt(defaultSettings.get(DEFAULT_ITEMS_QUERY_COUNT_KEY)),
            MINIMUM_ITEMS_QUERY_COUNT,
            NodeScope, Dynamic
    );

    Setting<String> ADMIN_ACCESS = Setting.simpleString(
            ADMIN_ACCESS_KEY,
            defaultSettings.get(ADMIN_ACCESS_KEY),
            NodeScope, Dynamic
    );

    Setting<String> FILTER_BY = Setting.simpleString(
            FILTER_BY_KEY,
            defaultSettings.get(FILTER_BY_KEY),
            NodeScope, Dynamic
    );

    Setting<List<String>> IGNORED_ROLES = Setting.listSetting(
            IGNORE_ROLE_KEY,
            DEFAULT_IGNORED_ROLES,
            (Function) null,
            NodeScope, Dynamic
    );

    /**
     * Returns list of additional settings available specific to this plugin.
     *
     * @return list of settings defined in this plugin
     */
    public List<Setting> getAllSettings() {
        ArrayList<Setting> allSettings = new ArrayList<>();
        allSettings.add(OPERATION_TIMEOUT_MS);
        allSettings.add(JOB_LOCK_DURATION_S);
        allSettings.add(MIN_POLLING_DURATION_S);
        allSettings.add(MAX_POLLING_DURATION_S);
        allSettings.add(MAX_LOCK_RETRIES);
        allSettings.add(DEFAULT_ITEMS_QUERY_COUNT);
        allSettings.add(ADMIN_ACCESS);
        allSettings.add(FILTER_BY);
        allSettings.add(IGNORED_ROLES);
        return allSettings;
    }


    /**
     * Update the setting variables to setting values from local settings
     *
     * @param clusterService cluster service instance
     */
    private void updateSettingValuesFromLocal(ClusterService clusterService) {
        operationTimeoutMs = OPERATION_TIMEOUT_MS.get(clusterService.getSettings());
        jobLockDurationSeconds = JOB_LOCK_DURATION_S.get(clusterService.getSettings());
        minPollingDurationSeconds = MIN_POLLING_DURATION_S.get(clusterService.getSettings());
        maxPollingDurationSeconds = MAX_POLLING_DURATION_S.get(clusterService.getSettings());
        maxLockRetries = MAX_LOCK_RETRIES.get(clusterService.getSettings());
        defaultItemsQueryCount = DEFAULT_ITEMS_QUERY_COUNT.get(clusterService.getSettings());
        adminAccess = AdminAccess.valueOf(ADMIN_ACCESS.get(clusterService.getSettings()));
        filterBy = FilterBy.valueOf(FILTER_BY.get(clusterService.getSettings()));
        ignoredRoles = IGNORED_ROLES.get(clusterService.getSettings());
    }

    /**
     * Update the setting variables to setting values from cluster settings
     *
     * @param clusterService cluster service instance
     */
    private void updateSettingValuesFromCluster(ClusterService clusterService) {
        Long clusterOperationTimeoutMs = clusterService.getClusterSettings().get(OPERATION_TIMEOUT_MS);
        if (clusterOperationTimeoutMs != null) {
            log.debug("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -autoUpdatedTo-> $clusterOperationTimeoutMs");
            operationTimeoutMs = clusterOperationTimeoutMs;
        }
        Integer clusterJobLockDurationSeconds = clusterService.getClusterSettings().get(JOB_LOCK_DURATION_S);
        if (clusterJobLockDurationSeconds != null) {
            log.debug("$LOG_PREFIX:$JOB_LOCK_DURATION_S_KEY -autoUpdatedTo-> $clusterJobLockDurationSeconds");
            jobLockDurationSeconds = clusterJobLockDurationSeconds;
        }
        Integer clusterMinPollingDurationSeconds = clusterService.getClusterSettings().get(MIN_POLLING_DURATION_S);
        if (clusterMinPollingDurationSeconds != null) {
            log.debug("$LOG_PREFIX:$MIN_POLLING_DURATION_S_KEY -autoUpdatedTo-> $clusterMinPollingDurationSeconds");
            minPollingDurationSeconds = clusterMinPollingDurationSeconds;
        }
        Integer clusterMaxPollingDurationSeconds = clusterService.getClusterSettings().get(MAX_POLLING_DURATION_S);
        if (clusterMaxPollingDurationSeconds != null) {
            log.debug("$LOG_PREFIX:$MAX_POLLING_DURATION_S_KEY -autoUpdatedTo-> $clusterMaxPollingDurationSeconds");
            maxPollingDurationSeconds = clusterMaxPollingDurationSeconds;
        }
        Integer clusterMaxLockRetries = clusterService.getClusterSettings().get(MAX_LOCK_RETRIES);
        if (clusterMaxLockRetries != null) {
            log.debug("$LOG_PREFIX:$MAX_LOCK_RETRIES_KEY -autoUpdatedTo-> $clusterMaxLockRetries");
            maxLockRetries = clusterMaxLockRetries;
        }
        Integer clusterDefaultItemsQueryCount = clusterService.getClusterSettings().get(DEFAULT_ITEMS_QUERY_COUNT);
        if (clusterDefaultItemsQueryCount != null) {
            log.debug("$LOG_PREFIX:$DEFAULT_ITEMS_QUERY_COUNT_KEY -autoUpdatedTo-> $clusterDefaultItemsQueryCount");
            defaultItemsQueryCount = clusterDefaultItemsQueryCount;
        }
        String clusterAdminAccess = clusterService.getClusterSettings().get(ADMIN_ACCESS);
        if (clusterAdminAccess != null) {
            log.debug("$LOG_PREFIX:$ADMIN_ACCESS_KEY -autoUpdatedTo-> $clusterAdminAccess");
            adminAccess = AdminAccess.valueOf(clusterAdminAccess);
        }
        String clusterFilterBy = clusterService.getClusterSettings().get(FILTER_BY);
        if (clusterFilterBy != null) {
            log.debug("$LOG_PREFIX:$FILTER_BY_KEY -autoUpdatedTo-> $clusterFilterBy");
            filterBy = FilterBy.valueOf(clusterFilterBy);
        }
        List<String> clusterIgnoredRoles = clusterService.getClusterSettings().get(IGNORED_ROLES);
        if (clusterIgnoredRoles != null) {
            log.debug("$LOG_PREFIX:$IGNORE_ROLE_KEY -autoUpdatedTo-> $clusterIgnoredRoles");
            ignoredRoles = clusterIgnoredRoles;
        }
    }

    /**
     * adds Settings update listeners to all settings.
     *
     * @param clusterService cluster service instance
     */
    void addSettingsUpdateConsumer(ClusterService clusterService) {
        updateSettingValuesFromLocal(clusterService);
        // Update the variables to cluster setting values
        // If the cluster is not yet started then we get default values again
        updateSettingValuesFromCluster(clusterService);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(OPERATION_TIMEOUT_MS, (Long it) -> {
            this.operationTimeoutMs = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JOB_LOCK_DURATION_S, (Integer it) -> {
            this.jobLockDurationSeconds = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MIN_POLLING_DURATION_S, (Integer it) -> {
            this.minPollingDurationSeconds = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_POLLING_DURATION_S, (Integer it) -> {
            this.maxPollingDurationSeconds = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_LOCK_RETRIES, (Integer it) -> {
            this.maxLockRetries = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_ITEMS_QUERY_COUNT, (Integer it) -> {
            this.defaultItemsQueryCount = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ADMIN_ACCESS, (String it) -> {
            this.adminAccess = AdminAccess.valueOf(it);
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY, (String it) -> {
            this.filterBy = FilterBy.valueOf(it);
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(IGNORED_ROLES, (List<String> it) -> {
            this.ignoredRoles = it;
            log.info("$LOG_PREFIX:$OPERATION_TIMEOUT_MS_KEY -updatedTo-> $it");
        });

    }

}






































