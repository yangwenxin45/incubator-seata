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
package org.apache.seata.rm.datasource.undo;

import org.apache.seata.common.Constants;
import org.apache.seata.common.util.CollectionUtils;
import org.apache.seata.common.util.SizeUtil;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.compressor.CompressorFactory;
import org.apache.seata.core.compressor.CompressorType;
import org.apache.seata.core.constants.ClientTableColumnsName;
import org.apache.seata.core.constants.ConfigurationKeys;
import org.apache.seata.core.exception.BranchTransactionException;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.rpc.processor.Pair;
import org.apache.seata.rm.datasource.ConnectionContext;
import org.apache.seata.rm.datasource.ConnectionProxy;
import org.apache.seata.rm.datasource.DataSourceProxy;
import org.apache.seata.rm.datasource.sql.struct.TableMetaCacheFactory;
import org.apache.seata.sqlparser.struct.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;
import java.util.*;

import static org.apache.seata.common.DefaultValues.*;
import static org.apache.seata.core.exception.TransactionExceptionCode.BranchRollbackFailed_Retriable;
import static org.apache.seata.core.exception.TransactionExceptionCode.BranchRollbackFailed_Unretriable;


public abstract class AbstractUndoLogManager implements UndoLogManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUndoLogManager.class);

    protected enum State {
        /**
         * This state can be properly rolled back by services
         */
        Normal(0),
        /**
         * This state prevents the branch transaction from inserting undo_log after the global transaction is rolled
         * back.
         */
        GlobalFinished(1);

        private int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    protected static final String UNDO_LOG_TABLE_NAME = ConfigurationFactory.getInstance().getConfig(
            ConfigurationKeys.TRANSACTION_UNDO_LOG_TABLE, DEFAULT_TRANSACTION_UNDO_LOG_TABLE);

    private static final String CHECK_UNDO_LOG_TABLE_EXIST_SQL = "SELECT 1 FROM " + UNDO_LOG_TABLE_NAME + " LIMIT 1";

    protected static final String SELECT_UNDO_LOG_SQL = "SELECT * FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
            + ClientTableColumnsName.UNDO_LOG_BRANCH_XID + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID
            + " = ? FOR UPDATE";

    protected static final String DELETE_UNDO_LOG_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
            + ClientTableColumnsName.UNDO_LOG_BRANCH_XID + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID + " = ?";

    protected static final String DELETE_SUB_UNDO_LOG_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
            + ClientTableColumnsName.UNDO_LOG_CONTEXT + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID + " = ?";

    protected static final boolean ROLLBACK_INFO_COMPRESS_ENABLE = ConfigurationFactory.getInstance().getBoolean(
            ConfigurationKeys.CLIENT_UNDO_COMPRESS_ENABLE, DEFAULT_CLIENT_UNDO_COMPRESS_ENABLE);

    protected static final CompressorType ROLLBACK_INFO_COMPRESS_TYPE = CompressorType.getByName(ConfigurationFactory.getInstance().getConfig(
            ConfigurationKeys.CLIENT_UNDO_COMPRESS_TYPE, DEFAULT_CLIENT_UNDO_COMPRESS_TYPE));

    protected static final long ROLLBACK_INFO_COMPRESS_THRESHOLD = SizeUtil.size2Long(ConfigurationFactory.getInstance().getConfig(
            ConfigurationKeys.CLIENT_UNDO_COMPRESS_THRESHOLD, DEFAULT_CLIENT_UNDO_COMPRESS_THRESHOLD));

    private static final ThreadLocal<String> SERIALIZER_LOCAL = new ThreadLocal<>();

    public static String getCurrentSerializer() {
        return SERIALIZER_LOCAL.get();
    }

    public static void setCurrentSerializer(String serializer) {
        SERIALIZER_LOCAL.set(serializer);
    }

    public static void removeCurrentSerializer() {
        SERIALIZER_LOCAL.remove();
    }

    /**
     * Delete undo log.
     *
     * @param xid      the xid
     * @param branchId the branch id
     * @param conn     the conn
     * @throws SQLException the sql exception
     */
    @Override
    public void deleteUndoLog(String xid, long branchId, Connection conn) throws SQLException {
        try (PreparedStatement deletePST = conn.prepareStatement(DELETE_UNDO_LOG_SQL);
             PreparedStatement deleteSubPST = conn.prepareStatement(DELETE_SUB_UNDO_LOG_SQL)) {
            deletePST.setLong(1, branchId);
            deletePST.setString(2, xid);
            deletePST.executeUpdate();

            deleteSubPST.setString(1, UndoLogConstants.BRANCH_ID_KEY + CollectionUtils.KV_SPLIT + branchId);
            deleteSubPST.setString(2, xid);
            deleteSubPST.executeUpdate();
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        }
    }

    /**
     * batch Delete undo log.
     *
     * @param xids      xid
     * @param branchIds branch Id
     * @param conn      connection
     */
    @Override
    public void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds, Connection conn) throws SQLException {
        // 如果 XID 集合或分支事务 ID 集合为空，则返回
        if (CollectionUtils.isEmpty(xids) || CollectionUtils.isEmpty(branchIds)) {
            return;
        }
        int xidSize = xids.size();
        int branchIdSize = branchIds.size();
        // 生成批量删除日志的 SQL 语句
        String batchDeleteSql = toBatchDeleteUndoLogSql(xidSize, branchIdSize);
        String batchDeleteSubSql = toBatchDeleteSubUndoLogSql(xidSize, branchIdSize);
        try (PreparedStatement deletePST = conn.prepareStatement(batchDeleteSql);
             PreparedStatement deleteSubPST = conn.prepareStatement(batchDeleteSubSql)) {
            int paramsIndex = 1;
            for (Long branchId : branchIds) {
                // 设置分支 ID
                deletePST.setLong(paramsIndex, branchId);
                deleteSubPST.setString(paramsIndex, UndoLogConstants.BRANCH_ID_KEY + CollectionUtils.KV_SPLIT + branchId);
                paramsIndex++;
            }
            for (String xid : xids) {
                // 设置 XID
                deletePST.setString(paramsIndex, xid);
                deleteSubPST.setString(paramsIndex, xid);
                paramsIndex++;
            }
            // 执行删除语句
            int deleteRows = deletePST.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch delete undo log size {}", deleteRows);
            }
            int deleteSubRows = deleteSubPST.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch delete sub undo log size {}", deleteSubRows);
            }
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        }
    }

    protected static String toBatchDeleteUndoLogSql(int xidSize, int branchIdSize) {
        StringBuilder sqlBuilder = new StringBuilder(64);
        sqlBuilder.append("DELETE FROM ").append(UNDO_LOG_TABLE_NAME).append(" WHERE  ").append(
                ClientTableColumnsName.UNDO_LOG_BRANCH_XID).append(" IN ");
        appendInParam(branchIdSize, sqlBuilder);
        sqlBuilder.append(" AND ").append(ClientTableColumnsName.UNDO_LOG_XID).append(" IN ");
        appendInParam(xidSize, sqlBuilder);
        return sqlBuilder.toString();
    }

    protected static String toBatchDeleteSubUndoLogSql(int xidSize, int branchIdSize) {
        StringBuilder sqlBuilder = new StringBuilder(64);
        sqlBuilder.append("DELETE FROM ").append(UNDO_LOG_TABLE_NAME).append(" WHERE  ").append(
                ClientTableColumnsName.UNDO_LOG_CONTEXT).append(" IN ");
        appendInParam(branchIdSize, sqlBuilder);
        sqlBuilder.append(" AND ").append(ClientTableColumnsName.UNDO_LOG_XID).append(" IN ");
        appendInParam(xidSize, sqlBuilder);
        return sqlBuilder.toString();
    }

    protected static void appendInParam(int size, StringBuilder sqlBuilder) {
        sqlBuilder.append(" (");
        for (int i = 0; i < size; i++) {
            sqlBuilder.append("?");
            if (i < (size - 1)) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(") ");
    }

    protected static boolean canUndo(int state) {
        return state == State.Normal.getValue();
    }

    protected String buildContext(String serializer, CompressorType compressorType, String... others) {
        Map<String, String> map = new HashMap<>(2, 1.01f);
        map.put(UndoLogConstants.SERIALIZER_KEY, serializer);
        map.put(UndoLogConstants.COMPRESSOR_TYPE_KEY, compressorType.name());
        if (others != null && others.length > 0 && others.length % 2 == 0) {
            for (int i = 0; i < others.length; ) {
                String key = others[i++];
                String value = others[i++];
                if (key != null) {
                    map.put(key, value == null ? "" : value);
                }
            }
        }
        return CollectionUtils.encodeMap(map);
    }

    protected Map<String, String> parseContext(String data) {
        return CollectionUtils.decodeMap(data);
    }

    /**
     * Flush undo logs.
     *
     * @param cp the cp
     * @throws SQLException the sql exception
     */
    @Override
    public void flushUndoLogs(ConnectionProxy cp) throws SQLException {
        // 获取连接上下文
        ConnectionContext connectionContext = cp.getContext();
        // 如果没有事务日志则返回
        if (!connectionContext.hasUndoLog()) {
            return;
        }

        String xid = connectionContext.getXid();
        long branchId = connectionContext.getBranchId();

        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid(xid);
        branchUndoLog.setBranchId(branchId);
        // 设置 SQL undolog
        branchUndoLog.setSqlUndoLogs(connectionContext.getUndoItems());

        // 获取分支事务日志解析器
        UndoLogParser parser = UndoLogParserFactory.getInstance();
        // 将分支事务日志编码为字节数组
        byte[] undoLogContent = parser.encode(branchUndoLog);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Flushing UNDO LOG: {}", new String(undoLogContent, Constants.DEFAULT_CHARSET));
        }

        CompressorType compressorType = CompressorType.NONE;
        // 如果需要压缩，则压缩字节数组
        if (needCompress(undoLogContent)) {
            compressorType = ROLLBACK_INFO_COMPRESS_TYPE;
            // 执行压缩算法
            undoLogContent = CompressorFactory.getCompressor(compressorType.getCode()).compress(undoLogContent);
        }
        String maxAllowedPacket = getMaxAllowedPacket(cp.getDataSourceProxy());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("resourceId: [{}] max_allowed_packet:[{}]", cp.getDataSourceProxy().getResourceId(), maxAllowedPacket);
        }
        String rollbackCtx = buildContext(
                parser.getName(), compressorType,
                UndoLogConstants.MAX_ALLOWED_PACKET, maxAllowedPacket
        );
        // 插入事务日志
        insertUndoLogWithNormal(xid, branchId, rollbackCtx, undoLogContent, cp.getTargetConnection());
    }

    /**
     * Undo.
     *
     * @param dataSourceProxy the data source proxy
     * @param xid             the xid
     * @param branchId        the branch id
     * @throws TransactionException the transaction exception
     */
    @Override
    public void undo(DataSourceProxy dataSourceProxy, String xid, long branchId) throws TransactionException {
        ConnectionProxy connectionProxy = null;
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement selectPST = null;
        // autoCommit 的当前状态
        boolean originalAutoCommit = true;

        for (; ; ) {
            try {
                connectionProxy = dataSourceProxy.getConnection();
                conn = connectionProxy.getTargetConnection();
                originalAutoCommit = conn.getAutoCommit();

                // The entire undo process should run in a local transaction.
                if (originalAutoCommit) {
                    // 如果 autocommit 本来为 true，则将其设置为 false
                    conn.setAutoCommit(false);
                }

                // Find UNDO LOG
                // 生成查询 undolog 的 PreparedStatement
                selectPST = conn.prepareStatement(buildSelectUndoSql());
                selectPST.setLong(1, branchId);
                selectPST.setString(2, xid);
                // 执行查询，得到结果集
                rs = selectPST.executeQuery();

                boolean exists = false;
                // 遍历找到的事务日志
                while (rs.next()) {
                    exists = true;

                    // It is possible that the server repeatedly sends a rollback request to roll back
                    // the same branch transaction to multiple processes,
                    // ensuring that only the undo_log in the normal state is processed.
                    int state = rs.getInt(ClientTableColumnsName.UNDO_LOG_LOG_STATUS);
                    if (!canUndo(state)) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("xid {} branch {}, ignore {} undo_log", xid, branchId, state);
                        }
                        return;
                    }

                    String contextString = rs.getString(ClientTableColumnsName.UNDO_LOG_CONTEXT);
                    Map<String, String> context = parseContext(contextString);
                    byte[] rollbackInfo = getRollbackInfo(rs);

                    String serializer = context == null ? null : context.get(UndoLogConstants.SERIALIZER_KEY);
                    UndoLogParser parser = serializer == null ? UndoLogParserFactory.getInstance()
                            : UndoLogParserFactory.getInstance(serializer);
                    // 解码 rollbackinfo 字段
                    BranchUndoLog branchUndoLog = parser.decode(rollbackInfo);

                    try {
                        // put serializer name to local
                        // 保存序列化器名称
                        setCurrentSerializer(parser.getName());
                        /**
                         * 一行 undo_log 数据代表一个分支事务，即业务侧的一个本地事务
                         * 一个本地事务可能包含多个 insert、update、delete 语句，各自对应一个 SQLUndoLog 对象
                         */
                        List<SQLUndoLog> sqlUndoLogs = branchUndoLog.getSqlUndoLogs();
                        if (sqlUndoLogs.size() > 1) {
                            // 事务的回滚需要从后往前依次进行，所以把顺序反转
                            Collections.reverse(sqlUndoLogs);
                        }
                        for (SQLUndoLog sqlUndoLog : sqlUndoLogs) {
                            // 获取表元数据
                            TableMeta tableMeta = TableMetaCacheFactory.getTableMetaCache(dataSourceProxy.getDbType()).getTableMeta(
                                    conn, sqlUndoLog.getTableName(), dataSourceProxy.getResourceId());
                            // 设置表元数据
                            sqlUndoLog.setTableMeta(tableMeta);
                            // 获取回滚执行器
                            AbstractUndoExecutor undoExecutor = UndoExecutorFactory.getUndoExecutor(
                                    dataSourceProxy.getDbType(), sqlUndoLog);
                            // 执行回滚
                            undoExecutor.executeOn(connectionProxy);
                        }
                    } finally {
                        // remove serializer name
                        // 删除保存的序列化器名称
                        removeCurrentSerializer();
                    }
                }

                // If undo_log exists, it means that the branch transaction has completed the first phase,
                // we can directly roll back and clean the undo_log
                // Otherwise, it indicates that there is an exception in the branch transaction,
                // causing undo_log not to be written to the database.
                // For example, the business processing timeout, the global transaction is the initiator rolls back.
                // To ensure data consistency, we can insert an undo_log with GlobalFinished state
                // to prevent the local transaction of the first phase of other programs from being correctly submitted.
                // See https://github.com/seata/seata/issues/489

                if (exists) {
                    // 删除 undolog
                    deleteUndoLog(xid, branchId, conn);
                    // 提交本地事务
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log deleted with {}", xid, branchId,
                                State.GlobalFinished.name());
                    }
                } else {
                    insertUndoLogWithGlobalFinished(xid, branchId, UndoLogParserFactory.getInstance(), conn);
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log added with {}", xid, branchId,
                                State.GlobalFinished.name());
                    }
                }

                return;
            } catch (SQLIntegrityConstraintViolationException e) {
                // Possible undo_log has been inserted into the database by other processes, retrying rollback undo_log
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("xid {} branch {}, undo_log inserted, retry rollback", xid, branchId);
                }
            } catch (Throwable e) {
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException rollbackEx) {
                        LOGGER.warn("Failed to close JDBC resource while undo ... ", rollbackEx);
                    }
                }
                if (e instanceof SQLUndoDirtyException) {
                    throw new BranchTransactionException(BranchRollbackFailed_Unretriable, String.format(
                            "Branch session rollback failed because of dirty undo log, please delete the relevant undolog after manually calibrating the data. xid = %s branchId = %s",
                            xid, branchId), e);
                }
                throw new BranchTransactionException(BranchRollbackFailed_Retriable,
                        String.format("Branch session rollback failed and try again later xid = %s branchId = %s %s", xid,
                                branchId, e.getMessage()),
                        e);

            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (selectPST != null) {
                        selectPST.close();
                    }
                    if (conn != null) {
                        if (originalAutoCommit) {
                            conn.setAutoCommit(true);
                        }
                        connectionProxy.close();
                    }
                } catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close JDBC resource while undo ... ", closeEx);
                }
            }
        }
    }

    /**
     * Construct a lock query sql
     *
     * @return sql
     */
    protected String buildSelectUndoSql() {
        return SELECT_UNDO_LOG_SQL;
    }

    /**
     * insert uodo log when global finished
     *
     * @param xid           the xid
     * @param branchId      the branchId
     * @param undoLogParser the undoLogParse
     * @param conn          sql connection
     * @throws SQLException SQLException
     */
    protected abstract void insertUndoLogWithGlobalFinished(String xid, long branchId, UndoLogParser undoLogParser,
                                                            Connection conn) throws SQLException;

    /**
     * insert uodo log when normal
     *
     * @param xid            the xid
     * @param branchId       the branchId
     * @param rollbackCtx    the rollbackContext
     * @param undoLogContent the undoLogContent
     * @param conn           sql connection
     * @throws SQLException SQLException
     */
    protected abstract void insertUndoLogWithNormal(String xid, long branchId, String rollbackCtx, byte[] undoLogContent,
                                                    Connection conn) throws SQLException;

    /**
     * get database server max allowed packet
     *
     * @param dataSourceProxy the datasource proxy
     * @return the max allowed packet value
     */
    protected String getMaxAllowedPacket(DataSourceProxy dataSourceProxy) {
        return StringUtils.EMPTY;
    }

    /**
     * RollbackInfo to bytes
     *
     * @param rs result set
     * @return rollback info
     * @throws SQLException SQLException
     */
    protected byte[] getRollbackInfo(ResultSet rs) throws SQLException {
        byte[] rollbackInfo = rs.getBytes(ClientTableColumnsName.UNDO_LOG_ROLLBACK_INFO);

        String rollbackInfoContext = rs.getString(ClientTableColumnsName.UNDO_LOG_CONTEXT);
        Map<String, String> context = CollectionUtils.decodeMap(rollbackInfoContext);
        String subIds = context.get(UndoLogConstants.SUB_ID_KEY);
        if (StringUtils.isNotBlank(subIds)) {
            Pair<Integer, List<byte[]>> pair = getSubRollbackInfo(rs.getStatement().getConnection(),
                    subIds, rs.getLong(ClientTableColumnsName.UNDO_LOG_BRANCH_XID),
                    rs.getString(ClientTableColumnsName.UNDO_LOG_XID));
            int total = pair.getFirst();
            byte[] rollbackInfoTotal = new byte[rollbackInfo.length + total];
            System.arraycopy(rollbackInfo, 0, rollbackInfoTotal, 0, rollbackInfo.length);
            int pos = rollbackInfo.length;
            for (byte[] bytes : pair.getSecond()) {
                System.arraycopy(bytes, 0, rollbackInfoTotal, pos, bytes.length);
                pos += bytes.length;
            }
            rollbackInfo = rollbackInfoTotal;
        }
        CompressorType compressorType = CompressorType.getByName(context.getOrDefault(UndoLogConstants.COMPRESSOR_TYPE_KEY,
                CompressorType.NONE.name()));
        return CompressorFactory.getCompressor(compressorType.getCode()).decompress(rollbackInfo);
    }

    @Override
    public int deleteUndoLogByLogCreated(Date logCreated, int limitRows, Connection conn) throws SQLException {
        return 0;
    }

    /**
     * get sub rollback info
     * @param conn the database connection
     * @param subIds sub rollback info id
     * @param branchId the branch id
     * @param xid the xid
     * @return first: sub rollback info size, seconds: rollback info bytes
     * @throws SQLException SQLException
     */
    protected Pair<Integer, List<byte[]>> getSubRollbackInfo(Connection conn, String subIds, Long branchId, String xid) throws SQLException {
        throw new UnsupportedOperationException("getSubRollbackInfo is not implemented");
    }

    /**
     * if the undoLogContent is big enough to be compress
     *
     * @param undoLogContent undoLogContent
     * @return boolean
     */
    protected boolean needCompress(byte[] undoLogContent) {
        return ROLLBACK_INFO_COMPRESS_ENABLE && undoLogContent.length > ROLLBACK_INFO_COMPRESS_THRESHOLD;
    }

    @Override
    public boolean hasUndoLogTable(Connection conn) {
        String checkExistSql = getCheckUndoLogTableExistSql();
        try (PreparedStatement checkPst = conn.prepareStatement(checkExistSql)) {
            checkPst.executeQuery();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    protected String getCheckUndoLogTableExistSql() {
        return CHECK_UNDO_LOG_TABLE_EXIST_SQL;
    }
}
