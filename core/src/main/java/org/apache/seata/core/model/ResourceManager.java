/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.core.model;

import java.util.Map;

/**
 * Resource Manager: common behaviors.
 *
 */
public interface ResourceManager extends ResourceManagerInbound, ResourceManagerOutbound {

    // 注册一个资源
    /**
     * Register a Resource to be managed by Resource Manager.
     *
     * @param resource The resource to be managed.
     */
    void registerResource(Resource resource);

    // 取消注册一个资源
    /**
     * Unregister a Resource from the Resource Manager.
     *
     * @param resource The resource to be removed.
     */
    void unregisterResource(Resource resource);

    // 获取管理的所有资源
    /**
     * Get all resources managed by this manager.
     *
     * @return resourceId -- Resource Map
     */
    Map<String, Resource> getManagedResources();

    // 获取分支事务类型，AT、TCC 等
    /**
     * Get the BranchType.
     *
     * @return The BranchType of ResourceManager.
     */
    BranchType getBranchType();

    /**
     * Get the GlobalStatus.
     *
     * @param branchType The BranchType of ResourceManager.
     * @param xid The xid of transaction.
     * @return The GlobalStatus of transaction.
     */
    GlobalStatus getGlobalStatus(BranchType branchType, String xid);
}
