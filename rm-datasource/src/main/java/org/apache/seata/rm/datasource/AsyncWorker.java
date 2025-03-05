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

import com.google.common.collect.Lists;
import org.apache.seata.common.thread.NamedThreadFactory;
import org.apache.seata.common.util.IOUtil;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.rm.datasource.undo.UndoLogManager;
import org.apache.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.seata.common.DefaultValues.DEFAULT_CLIENT_ASYNC_COMMIT_BUFFER_LIMIT;
import static org.apache.seata.core.constants.ConfigurationKeys.CLIENT_ASYNC_COMMIT_BUFFER_LIMIT;

/**
 * The type Async worker.
 *
 */
public class AsyncWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWorker.class);

    private static final int DEFAULT_RESOURCE_SIZE = 16;

    private static final int UNDOLOG_DELETE_LIMIT_SIZE = 1000;

    private static final int ASYNC_COMMIT_BUFFER_LIMIT = ConfigurationFactory.getInstance().getInt(
        CLIENT_ASYNC_COMMIT_BUFFER_LIMIT, DEFAULT_CLIENT_ASYNC_COMMIT_BUFFER_LIMIT);

    private final DataSourceManager dataSourceManager;

    private final BlockingQueue<Phase2Context> commitQueue;

    private final ScheduledExecutorService scheduledExecutor;

    public AsyncWorker(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;

        LOGGER.info("Async Commit Buffer Limit: {}", ASYNC_COMMIT_BUFFER_LIMIT);
        commitQueue = new LinkedBlockingQueue<>(ASYNC_COMMIT_BUFFER_LIMIT);

        ThreadFactory threadFactory = new NamedThreadFactory("AsyncWorker", 2, true);
        scheduledExecutor = new ScheduledThreadPoolExecutor(2, threadFactory);
        scheduledExecutor.scheduleAtFixedRate(this::doBranchCommitSafely, 10, 1000, TimeUnit.MILLISECONDS);
    }

    public BranchStatus branchCommit(String xid, long branchId, String resourceId) {
        // 二阶段的上下文
        Phase2Context context = new Phase2Context(xid, branchId, resourceId);
        // 添加到提交队列
        addToCommitQueue(context);
        // 返回二阶段提交成功
        return BranchStatus.PhaseTwo_Committed;
    }

    /**
     * try add context to commitQueue directly, if fail(which means the queue is full),
     * then doBranchCommit urgently(so that the queue could be empty again) and retry this process.
     */
    private void addToCommitQueue(Phase2Context context) {
        if (commitQueue.offer(context)) {
            return;
        }
        CompletableFuture.runAsync(this::doBranchCommitSafely, scheduledExecutor)
                .thenRun(() -> addToCommitQueue(context));
    }

    private void addAllToCommitQueue(List<Phase2Context> contexts) {
        for (Phase2Context context : contexts) {
            addToCommitQueue(context);
        }
    }

    void doBranchCommitSafely() {
        try {
            doBranchCommit();
        } catch (Throwable e) {
            LOGGER.error("Exception occur when doing branch commit", e);
        }
    }

    private void doBranchCommit() {
        // 如果队列为空，则返回
        if (commitQueue.isEmpty()) {
            return;
        }

        // transfer all context currently received to this list
        List<Phase2Context> allContexts = new LinkedList<>();
        // 把队列中的二阶段上下文放到列表中
        commitQueue.drainTo(allContexts);

        // group context by their resourceId
        // 按照资源 ID 进行分组
        Map<String, List<Phase2Context>> groupedContexts = groupedByResourceId(allContexts);

        // 处理每个分组
        groupedContexts.forEach(this::dealWithGroupedContexts);
    }

    Map<String, List<Phase2Context>> groupedByResourceId(List<Phase2Context> contexts) {
        Map<String, List<Phase2Context>> groupedContexts = new HashMap<>(DEFAULT_RESOURCE_SIZE);
        contexts.forEach(context -> {
            if (StringUtils.isBlank(context.resourceId)) {
                LOGGER.warn("resourceId is empty, resource:{}", context);
                return;
            }
            List<Phase2Context> group = groupedContexts.computeIfAbsent(context.resourceId, key -> new LinkedList<>());
            group.add(context);
        });
        return groupedContexts;
    }

    private void dealWithGroupedContexts(String resourceId, List<Phase2Context> contexts) {
        if (StringUtils.isBlank(resourceId)) {
            //ConcurrentHashMap required notNull key
            LOGGER.warn("resourceId is empty and will skip.");
            return;
        }
        // 根据资源 ID 取得数据源代理
        DataSourceProxy dataSourceProxy = dataSourceManager.get(resourceId);
        // 如果数据源代理为空则返回
        if (dataSourceProxy == null) {
            LOGGER.warn("failed to find resource for {} and requeue", resourceId);
            addAllToCommitQueue(contexts);
            return;
        }

        Connection conn = null;
        try {
            // 获取普通的数据库连接
            conn = dataSourceProxy.getPlainConnection();
            // 得到 undolog 管理器
            UndoLogManager undoLogManager = UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType());

            // split contexts into several lists, with each list contain no more element than limit size
            // 把二阶段上下文列表拆分成多个小列表，目的是防止列表过大，造成拼接出的 SQL 语句过长，会超出数据库的限制而失败
            List<List<Phase2Context>> splitByLimit = Lists.partition(contexts, UNDOLOG_DELETE_LIMIT_SIZE);
            for (List<Phase2Context> partition : splitByLimit) {
                // 对每个小列表处理，调用 deleteUndoLog 方法删除一批分支事务的日志
                deleteUndoLog(conn, undoLogManager, partition);
            }
        } catch (SQLException sqlExx) {
            addAllToCommitQueue(contexts);
            LOGGER.error("failed to get connection for async committing on {} and requeue", resourceId, sqlExx);
        } finally {
            IOUtil.close(conn);
        }

    }

    private void deleteUndoLog(final Connection conn, UndoLogManager undoLogManager, List<Phase2Context> contexts) {
        // xid 集合
        Set<String> xids = new LinkedHashSet<>(contexts.size());
        // 分支事务 ID 集合
        Set<Long> branchIds = new LinkedHashSet<>(contexts.size());
        // 把二阶段上下文中的 XID 和分支事务 ID 分别加入上面的两个集合中
        contexts.forEach(context -> {
            xids.add(context.xid);
            branchIds.add(context.branchId);
        });

        try {
            // 批量删除事务日志
            undoLogManager.batchDeleteUndoLog(xids, branchIds, conn);
            // 提交本地事务
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to batch delete undo log", e);
            try {
                // 如果出现 SQL 异常，则会滚本地事务
                conn.rollback();
                addAllToCommitQueue(contexts);
            } catch (SQLException rollbackEx) {
                LOGGER.error("Failed to rollback JDBC resource after deleting undo log failed", rollbackEx);
            }
        }
    }

    static class Phase2Context {

        /**
         * AT Phase 2 context
         * @param xid             the xid
         * @param branchId        the branch id
         * @param resourceId      the resource id
         */
        public Phase2Context(String xid, long branchId, String resourceId) {
            this.xid = xid;
            this.branchId = branchId;
            this.resourceId = resourceId;
        }

        /**
         * The Xid.
         */
        String xid;
        /**
         * The Branch id.
         */
        long branchId;
        /**
         * The Resource id.
         */
        String resourceId;

        @Override
        public String toString() {
            return "Phase2Context{" + "xid='" + xid + '\'' + ", branchId=" + branchId + ", resourceId='" + resourceId
                + '\'' + '}';
        }
    }
}
