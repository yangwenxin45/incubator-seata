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

import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.constants.ConfigurationKeys;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.exception.TransactionExceptionCode;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.DefaultResourceManager;
import org.apache.seata.rm.datasource.exec.LockConflictException;
import org.apache.seata.rm.datasource.exec.LockRetryController;
import org.apache.seata.rm.datasource.undo.SQLUndoLog;
import org.apache.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.Callable;

import static org.apache.seata.common.DefaultValues.*;

/**
 * The type Connection proxy.
 *
 */
public class ConnectionProxy extends AbstractConnectionProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxy.class);

    private final ConnectionContext context = new ConnectionContext();

    private final LockRetryPolicy lockRetryPolicy = new LockRetryPolicy(this);

    private static final int REPORT_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_REPORT_RETRY_COUNT, DEFAULT_CLIENT_REPORT_RETRY_COUNT);

    public static final boolean IS_REPORT_SUCCESS_ENABLE = ConfigurationFactory.getInstance().getBoolean(
        ConfigurationKeys.CLIENT_REPORT_SUCCESS_ENABLE, DEFAULT_CLIENT_REPORT_SUCCESS_ENABLE);

    /**
     * Instantiates a new Connection proxy.
     *
     * @param dataSourceProxy  the data source proxy
     * @param targetConnection the target connection
     */
    public ConnectionProxy(DataSourceProxy dataSourceProxy, Connection targetConnection) {
        super(dataSourceProxy, targetConnection);
    }

    /**
     * Gets context.
     *
     * @return the context
     */
    public ConnectionContext getContext() {
        return context;
    }

    /**
     * Bind.
     *
     * @param xid the xid
     */
    public void bind(String xid) {
        context.bind(xid);
    }

    /**
     * set global lock requires flag
     *
     * @param isLock whether to lock
     */
    public void setGlobalLockRequire(boolean isLock) {
        context.setGlobalLockRequire(isLock);
    }

    /**
     * get global lock requires flag
     *
     * @return the boolean
     */
    public boolean isGlobalLockRequire() {
        return context.isGlobalLockRequire();
    }

    /**
     * Check lock.
     *
     * @param lockKeys the lockKeys
     * @throws SQLException the sql exception
     */
    public void checkLock(String lockKeys) throws SQLException {
        // 如果加锁数据为空则返回
        if (StringUtils.isBlank(lockKeys)) {
            return;
        }
        // Just check lock without requiring lock by now.
        try {
            // 检查 Seata 全局锁
            boolean lockable = DefaultResourceManager.get().lockQuery(BranchType.AT,
                getDataSourceProxy().getResourceId(), context.getXid(), lockKeys);
            // 如果锁已经占用，则抛出锁冲突异常
            if (!lockable) {
                throw new LockConflictException(String.format("get lock failed, lockKey: %s",lockKeys));
            }
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, lockKeys);
        }
    }

    /**
     * Lock query.
     *
     * @param lockKeys the lock keys
     * @return the boolean
     * @throws SQLException the sql exception
     */
    public boolean lockQuery(String lockKeys) throws SQLException {
        // Just check lock without requiring lock by now.
        boolean result = false;
        try {
            result = DefaultResourceManager.get().lockQuery(BranchType.AT, getDataSourceProxy().getResourceId(),
                context.getXid(), lockKeys);
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, lockKeys);
        }
        return result;
    }

    private void recognizeLockKeyConflictException(TransactionException te) throws SQLException {
        recognizeLockKeyConflictException(te, null);
    }

    private void recognizeLockKeyConflictException(TransactionException te, String lockKeys) throws SQLException {
        if (te.getCode() == TransactionExceptionCode.LockKeyConflict
            || te.getCode() == TransactionExceptionCode.LockKeyConflictFailFast) {
            StringBuilder reasonBuilder = new StringBuilder("get global lock fail, xid:");
            reasonBuilder.append(context.getXid());
            if (StringUtils.isNotBlank(lockKeys)) {
                reasonBuilder.append(", lockKeys:").append(lockKeys);
            }
            throw new LockConflictException(reasonBuilder.toString(), te.getCode());
        } else {
            throw new SQLException(te);
        }

    }

    /**
     * append sqlUndoLog
     *
     * @param sqlUndoLog the sql undo log
     */
    public void appendUndoLog(SQLUndoLog sqlUndoLog) {
        context.appendUndoItem(sqlUndoLog);
    }

    /**
     * append lockKey
     *
     * @param lockKey the lock key
     */
    public void appendLockKey(String lockKey) {
        context.appendLockKey(lockKey);
    }

    @Override
    public void commit() throws SQLException {
        try {
            // 锁冲突重试机制
            lockRetryPolicy.execute(() -> {
                // 提交本地事务
                doCommit();
                return null;
            });
        } catch (SQLException e) {
            if (targetConnection != null && !getAutoCommit() && !getContext().isAutoCommitChanged()) {
                // 回滚本地事务
                rollback();
            }
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        Savepoint savepoint = targetConnection.setSavepoint();
        context.appendSavepoint(savepoint);
        return savepoint;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        Savepoint savepoint = targetConnection.setSavepoint(name);
        context.appendSavepoint(savepoint);
        return savepoint;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        targetConnection.rollback(savepoint);
        context.removeSavepoint(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        targetConnection.releaseSavepoint(savepoint);
        context.releaseSavepoint(savepoint);
    }


    private void doCommit() throws SQLException {
        // 是否参与全局事务
        if (context.inGlobalTransaction()) {
            // 分支事务提交
            processGlobalTransactionCommit();
            // 没有参与全局事务，判断是否有查询全局锁请求
        } else if (context.isGlobalLockRequire()) {
            // 本地事务提交并查询全局锁
            processLocalCommitWithGlobalLocks();
        } else {
            // 本地事务提交
            targetConnection.commit();
        }
    }

    private void processLocalCommitWithGlobalLocks() throws SQLException {
        // 检查 Seata 全局锁
        checkLock(context.buildLockKeys());
        try {
            // 提交本地事务
            targetConnection.commit();
        } catch (Throwable ex) {
            throw new SQLException(ex);
        }
        // 重置上下文
        context.reset();
    }

    /**
     * 1. 注册分支事务
     * 2. 保存事务日志
     * 3. 本地事务提交
     * 4. 上报分支事务状态
     *
     * @author yangwenxin
     * @date 2025-03-03 11:23
     */
    private void processGlobalTransactionCommit() throws SQLException {
        try {
            // 向事务协调器注册分支事务
            register();
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, context.buildLockKeys());
        }
        try {
            // 保存事务日志
            UndoLogManagerFactory.getUndoLogManager(this.getDbType()).flushUndoLogs(this);
            // 提交本地事务，插入 undolog 与业务 SQL 语句在同一个本地事务中
            targetConnection.commit();
        } catch (Throwable ex) {
            LOGGER.error("process connectionProxy commit error: {}", ex.getMessage(), ex);
            // 向事务协调器上报分支事务状态为"失败"
            report(false);
            throw new SQLException(ex);
        }
        if (IS_REPORT_SUCCESS_ENABLE) {
            // 向事务协调器上报分支事务状态为"成功"
            report(true);
        }
        // 重置事务上下文
        context.reset();
    }

    private void register() throws TransactionException {
        // 如果没有事务日志或者需要加全局锁的 key，则返回
        if (!context.hasUndoLog() || !context.hasLockKey()) {
            return;
        }

        // 分支注册
        Long branchId = DefaultResourceManager.get().branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(),
            null, context.getXid(), context.getApplicationData(),
            context.buildLockKeys());
        // 设置分支 ID 到上下文
        context.setBranchId(branchId);
    }

    @Override
    public void rollback() throws SQLException {
        targetConnection.rollback();
        if (context.inGlobalTransaction() && context.isBranchRegistered()) {
            report(false);
        }
        context.reset();
    }

    /**
     * change connection autoCommit to false by seata
     *
     * @throws SQLException the sql exception
     */
    public void changeAutoCommit() throws SQLException {
        getContext().setAutoCommitChanged(true);
        setAutoCommit(false);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if ((context.inGlobalTransaction() || context.isGlobalLockRequire()) && autoCommit && !getAutoCommit()) {
            // change autocommit from false to true, we should commit() first according to JDBC spec.
            doCommit();
        }
        targetConnection.setAutoCommit(autoCommit);
    }

    private void report(boolean commitDone) throws SQLException {
        // 如果分支事务 ID 为空则返回
        if (context.getBranchId() == null) {
            return;
        }
        // 默认最大重试次数为 5
        int retry = REPORT_RETRY_COUNT;
        while (retry > 0) {
            try {
                // 上报分支状态
                DefaultResourceManager.get().branchReport(BranchType.AT, context.getXid(), context.getBranchId(),
                    commitDone ? BranchStatus.PhaseOne_Done : BranchStatus.PhaseOne_Failed, null);
                return;
            } catch (Throwable ex) {
                LOGGER.error("Failed to report [" + context.getBranchId() + "/" + context.getXid() + "] commit done ["
                    + commitDone + "] Retry Countdown: " + retry);
                // 可重试次数减一
                retry--;

                if (retry == 0) {
                    throw new SQLException("Failed to report branch status " + commitDone, ex);
                }
            }
        }
    }

    public static class LockRetryPolicy {
        // 读取配置，如果是锁冲突，则直接回滚或重试，默认是直接回滚
        protected static final boolean LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT = ConfigurationFactory
                .getInstance().getBoolean(ConfigurationKeys.CLIENT_LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT, DEFAULT_CLIENT_LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT);

        protected final ConnectionProxy connection;

        public LockRetryPolicy(ConnectionProxy connection) {
            this.connection = connection;
        }

        public <T> T execute(Callable<T> callable) throws Exception {
            // the only case that not need to retry acquire lock hear is
            //    LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT == true && connection#autoCommit == true
            // because it has retry acquire lock when AbstractDMLBaseExecutor#executeAutoCommitTrue
            if (LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT && connection.getContext().isAutoCommitChanged()) {
                // 执行一次，不会重试
                return callable.call();
            } else {
                // LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT == false
                // or LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT == true && autoCommit == false
                // 重试
                return doRetryOnLockConflict(callable);
            }
        }

        protected <T> T doRetryOnLockConflict(Callable<T> callable) throws Exception {
            // 重试控制器
            LockRetryController lockRetryController = new LockRetryController();
            while (true) {
                try {
                    return callable.call();
                } catch (LockConflictException lockConflict) {
                    // 异常忽略
                    onException(lockConflict);
                    // AbstractDMLBaseExecutor#executeAutoCommitTrue the local lock is released
                    if (connection.getContext().isAutoCommitChanged()
                        && lockConflict.getCode() == TransactionExceptionCode.LockKeyConflictFailFast) {
                        lockConflict.setCode(TransactionExceptionCode.LockKeyConflict);
                    }
                    // 休眠后重试
                    lockRetryController.sleep(lockConflict);
                } catch (Exception e) {
                    onException(e);
                    // 如果是非锁冲突异常，则抛出异常退出循环
                    throw e;
                }
            }
        }

        /**
         * Callback on exception in doLockRetryOnConflict.
         *
         * @param e invocation exception
         * @throws Exception error
         */
        protected void onException(Exception e) throws Exception {
        }
    }
}
