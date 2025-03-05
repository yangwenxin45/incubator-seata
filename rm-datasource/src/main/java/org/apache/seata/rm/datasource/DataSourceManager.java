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
package org.apache.seata.rm.datasource;

import org.apache.seata.common.exception.NotSupportYetException;
import org.apache.seata.common.exception.ShouldNeverHappenException;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.exception.RmTransactionException;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.exception.TransactionExceptionCode;
import org.apache.seata.core.logger.StackTraceLogger;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.Resource;
import org.apache.seata.core.protocol.ResultCode;
import org.apache.seata.core.protocol.transaction.GlobalLockQueryRequest;
import org.apache.seata.core.protocol.transaction.GlobalLockQueryResponse;
import org.apache.seata.core.rpc.netty.RmNettyRemotingClient;
import org.apache.seata.rm.AbstractResourceManager;
import org.apache.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * The type Data source manager.
 */
public class DataSourceManager extends AbstractResourceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceManager.class);

    private final AsyncWorker asyncWorker = new AsyncWorker(this);

    // 数据源缓存映射
    private final Map<String, Resource> dataSourceCache = new ConcurrentHashMap<>();

    @Override
    public boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys) throws TransactionException {
        GlobalLockQueryRequest request = new GlobalLockQueryRequest();
        request.setXid(xid);
        request.setLockKey(lockKeys);
        request.setResourceId(resourceId);
        try {
            GlobalLockQueryResponse response;
            if (RootContext.inGlobalTransaction() || RootContext.requireGlobalLock()) {
                response = (GlobalLockQueryResponse) RmNettyRemotingClient.getInstance().sendSyncRequest(request);
            } else {
                throw new RuntimeException("unknow situation!");
            }

            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(),
                    "Response[" + response.getMsg() + "]");
            }
            return response.isLockable();
        } catch (TimeoutException toe) {
            throw new RmTransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new RmTransactionException(TransactionExceptionCode.LockableCheckFailed, "Runtime", rex);
        }
    }

    /**
     * Instantiates a new Data source manager.
     */
    public DataSourceManager() {
    }

    @Override
    public void registerResource(Resource resource) {
        DataSourceProxy dataSourceProxy = (DataSourceProxy) resource;
        // 加入本地缓存
        dataSourceCache.put(dataSourceProxy.getResourceId(), dataSourceProxy);
        // 调用父类注册资源
        super.registerResource(dataSourceProxy);
    }

    @Override
    public void unregisterResource(Resource resource) {
        throw new NotSupportYetException("unregister a resource");
    }

    /**
     * Get data source proxy.
     *
     * @param resourceId the resource id
     * @return the data source proxy
     */
    public DataSourceProxy get(String resourceId) {
        return (DataSourceProxy) dataSourceCache.get(resourceId);
    }

    @Override
    public BranchStatus branchCommit(BranchType branchType, String xid, long branchId, String resourceId,
                                     String applicationData) throws TransactionException {
        // 通过异步线程进行分支事务的两阶段提交
        return asyncWorker.branchCommit(xid, branchId, resourceId);
    }

    @Override
    public BranchStatus branchRollback(BranchType branchType, String xid, long branchId, String resourceId,
                                       String applicationData) throws TransactionException {
        // 通过资源 ID 得到数据源代理
        DataSourceProxy dataSourceProxy = get(resourceId);
        if (dataSourceProxy == null) {
            throw new ShouldNeverHappenException(String.format("resource: %s not found",resourceId));
        }
        try {
            // 调用 undo() 方法实现分支事务回滚
            UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType()).undo(dataSourceProxy, xid, branchId);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("branch rollback success, xid:{}, branchId:{}", xid, branchId);
            }
        } catch (TransactionException te) {
            StackTraceLogger.error(LOGGER, te,
                "branchRollback failed. branchType:[{}], xid:[{}], branchId:[{}], resourceId:[{}], applicationData:[{}]. reason:[{}]",
                new Object[]{branchType, xid, branchId, resourceId, applicationData, te.getMessage()});
            if (te.getCode() == TransactionExceptionCode.BranchRollbackFailed_Unretriable) {
                // 二阶段回滚失败，并且不可重试（发生逻辑错误，重试没有意义，需要人工介入）
                return BranchStatus.PhaseTwo_RollbackFailed_Unretryable;
            } else {
                // 二阶段回滚失败，并且可重试（重试有可能成功）
                return BranchStatus.PhaseTwo_RollbackFailed_Retryable;
            }
        }
        // 分支回滚成功
        return BranchStatus.PhaseTwo_Rollbacked;

    }

    @Override
    public Map<String, Resource> getManagedResources() {
        return dataSourceCache;
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.AT;
    }

}
