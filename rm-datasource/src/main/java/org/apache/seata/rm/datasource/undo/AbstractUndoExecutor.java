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

import com.alibaba.fastjson.JSON;
import org.apache.seata.common.util.IOUtil;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.constants.ConfigurationKeys;
import org.apache.seata.core.model.Result;
import org.apache.seata.rm.datasource.ConnectionProxy;
import org.apache.seata.rm.datasource.DataCompareUtils;
import org.apache.seata.rm.datasource.SqlGenerateUtils;
import org.apache.seata.rm.datasource.sql.serial.SerialArray;
import org.apache.seata.rm.datasource.sql.struct.Field;
import org.apache.seata.rm.datasource.sql.struct.KeyType;
import org.apache.seata.rm.datasource.sql.struct.Row;
import org.apache.seata.rm.datasource.sql.struct.TableRecords;
import org.apache.seata.sqlparser.struct.TableMeta;
import org.apache.seata.sqlparser.util.ColumnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialDatalink;
import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seata.common.DefaultValues.DEFAULT_TRANSACTION_UNDO_DATA_VALIDATION;

/**
 * The type Abstract undo executor.
 *
 */
public abstract class AbstractUndoExecutor {

    /**
     * Logger for AbstractUndoExecutor
     **/
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUndoExecutor.class);

    /**
     * template of check sql
     * TODO support multiple primary key
     */
    private static final String CHECK_SQL_TEMPLATE = "SELECT * FROM %s WHERE %s FOR UPDATE";

    /**
     * Switch of undo data validation
     */
    public static final boolean IS_UNDO_DATA_VALIDATION_ENABLE = ConfigurationFactory.getInstance()
            .getBoolean(ConfigurationKeys.TRANSACTION_UNDO_DATA_VALIDATION, DEFAULT_TRANSACTION_UNDO_DATA_VALIDATION);

    /**
     * The Sql undo log.
     */
    protected SQLUndoLog sqlUndoLog;

    /**
     * Build undo sql string.
     *
     * @return the string
     */
    protected abstract String buildUndoSQL();

    /**
     * Instantiates a new Abstract undo executor.
     *
     * @param sqlUndoLog the sql undo log
     */
    public AbstractUndoExecutor(SQLUndoLog sqlUndoLog) {
        this.sqlUndoLog = sqlUndoLog;
    }

    /**
     * Gets sql undo log.
     *
     * @return the sql undo log
     */
    public SQLUndoLog getSqlUndoLog() {
        return sqlUndoLog;
    }

    /**
     * Execute on.
     *
     * @param connectionProxy the connection proxy
     * @throws SQLException the sql exception
     */
    public void executeOn(ConnectionProxy connectionProxy) throws SQLException {
        // 如果"脏写"检查开关是打开的，则需要校验"脏写"
        Connection conn = connectionProxy.getTargetConnection();
        if (IS_UNDO_DATA_VALIDATION_ENABLE && !dataValidationAndGoOn(connectionProxy)) {
            // 如果有"脏写"，则不能继续执行回滚，需要人工排查问题
            return;
        }
        PreparedStatement undoPST = null;
        try {
            // 构建回滚语句
            String undoSQL = buildUndoSQL();
            undoPST = conn.prepareStatement(undoSQL);
            // 得到所有需要回滚的行
            TableRecords undoRows = getUndoRows();
            // 遍历所有行
            for (Row undoRow : undoRows.getRows()) {
                ArrayList<Field> undoValues = new ArrayList<>();
                // 得到主键列表
                List<Field> pkValueList = getOrderedPkList(undoRows, undoRow, connectionProxy.getDbType());
                for (Field field : undoRow.getFields()) {
                    // 将非主键的字段加入列表
                    if (field.getKeyType() != KeyType.PRIMARY_KEY) {
                        undoValues.add(field);
                    }
                }

                // 给 PreparedStatement 赋值
                undoPrepare(undoPST, undoValues, pkValueList);

                // 执行 PreparedStatement
                undoPST.executeUpdate();
            }

        } catch (Exception ex) {
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            } else {
                throw new SQLException(ex);
            }
        }
        finally {
            //important for oracle
            IOUtil.close(undoPST);
        }

    }

    /**
     * Undo prepare.
     *
     * @param undoPST     the undo pst
     * @param undoValues  the undo values
     * @param pkValueList the pk value
     * @throws SQLException the sql exception
     */
    protected void undoPrepare(PreparedStatement undoPST, ArrayList<Field> undoValues, List<Field> pkValueList)
            throws SQLException {
        int undoIndex = 0;
        // 遍历所有的字段
        for (Field undoValue : undoValues) {
            undoIndex++;
            // 字段类型
            int type = undoValue.getType();
            // 字段值
            Object value = undoValue.getValue();
            // 根据不同类型设置 PreparedStatement
            if (type == JDBCType.BLOB.getVendorTypeNumber()) {
                // Blob 类型
                SerialBlob serialBlob = (SerialBlob) value;
                if (serialBlob != null) {
                    undoPST.setObject(undoIndex, serialBlob.getBinaryStream());
                } else {
                    undoPST.setObject(undoIndex, null);
                }
            } else if (type == JDBCType.LONGVARBINARY.getVendorTypeNumber()) {
                if (value != null) {
                    byte[] bytes = (byte[]) value;
                    undoPST.setObject(undoIndex, new ByteArrayInputStream(bytes));
                } else {
                    undoPST.setObject(undoIndex, null);
                }
            } else if (type == JDBCType.CLOB.getVendorTypeNumber() || type == JDBCType.NCLOB.getVendorTypeNumber()) {
                // clob 类型
                SerialClob serialClob = (SerialClob) value;
                if (serialClob != null) {
                    undoPST.setClob(undoIndex, serialClob.getCharacterStream());
                } else {
                    undoPST.setObject(undoIndex, null);
                }
            } else if (type == JDBCType.DATALINK.getVendorTypeNumber()) {
                SerialDatalink dataLink = (SerialDatalink) value;
                if (dataLink != null) {
                    undoPST.setURL(undoIndex, dataLink.getDatalink());
                } else {
                    undoPST.setObject(undoIndex, null);
                }
            } else if (type == JDBCType.ARRAY.getVendorTypeNumber()) {
                SerialArray array = (SerialArray) value;
                if (array != null) {
                    Array arrayOf = undoPST.getConnection().createArrayOf(array.getBaseTypeName(), array.getElements());
                    undoPST.setArray(undoIndex, arrayOf);
                } else {
                    undoPST.setObject(undoIndex, null);
                }
            } else if (undoValue.getType() == JDBCType.OTHER.getVendorTypeNumber()) {
                undoPST.setObject(undoIndex, value);
            } else if (undoValue.getType() == JDBCType.BIT.getVendorTypeNumber()) {
                undoPST.setObject(undoIndex, value);
            } else {
                // JDBCType.REF, JDBCType.JAVA_OBJECT etc...
                undoPST.setObject(undoIndex, value, type);
            }
        }
        // PK is always at last.
        // INSERT INTO a (x, y, z, pk1,pk2) VALUES (?, ?, ?, ? ,?)
        // UPDATE a SET x=?, y=?, z=? WHERE pk1 in (?) and pk2 in (?)
        // DELETE FROM a WHERE pk1 in (?) and pk2 in (?)
        for (Field pkField : pkValueList) {
            undoIndex++;
            undoPST.setObject(undoIndex, pkField.getValue(), pkField.getType());
        }

    }

    /**
     * Gets undo rows.
     *
     * @return the undo rows
     */
    protected abstract TableRecords getUndoRows();

    /**
     * 判断逻辑如下：
     * 1. 如果当前值与后镜像相同，则无"脏写"，不回滚
     * 2. 如果当前值与后镜像不同，但与前镜像相同，则无"脏写"，不会滚，因为值已经是我们期望的
     * 3. 如果当前值与前后镜像都不相同，则发生了"脏写"，会抛出异常，需要人工修正
     *
     * @author yangwenxin
     * @date 2025-03-04 16:09
     */
    /**
     * Data validation.
     *
     * @param conn the conn
     * @return return true if data validation is ok and need continue undo, and return false if no need continue undo.
     * @throws SQLException the sql exception such as has dirty data
     */
    protected boolean dataValidationAndGoOn(ConnectionProxy conn) throws SQLException {

        // 得到前镜像
        TableRecords beforeRecords = sqlUndoLog.getBeforeImage();
        // 得到后镜像
        TableRecords afterRecords = sqlUndoLog.getAfterImage();

        // Compare current data with before data
        // No need undo if the before data snapshot is equivalent to the after data snapshot.
        // 比较前后镜像是否相同
        Result<Boolean> beforeEqualsAfterResult = DataCompareUtils.isRecordsEquals(beforeRecords, afterRecords);
        if (beforeEqualsAfterResult.getResult()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Stop rollback because there is no data change " +
                        "between the before data snapshot and the after data snapshot.");
            }
            // no need continue undo.
            // 相同则不回滚
            return false;
        }

        // Validate if data is dirty.
        // 查询当前值
        TableRecords currentRecords = queryCurrentRecords(conn);
        // compare with current data and after image.
        // 比较当前值与后镜像是否相同
        Result<Boolean> afterEqualsCurrentResult = DataCompareUtils.isRecordsEquals(afterRecords, currentRecords);
        if (!afterEqualsCurrentResult.getResult()) {

            // If current data is not equivalent to the after data, then compare the current data with the before
            // data, too. No need continue to undo if current data is equivalent to the before data snapshot
            // 如果当前值与后镜像不相同，则继续比较当前值与前镜像是否相同
            Result<Boolean> beforeEqualsCurrentResult = DataCompareUtils.isRecordsEquals(beforeRecords, currentRecords);
            if (beforeEqualsCurrentResult.getResult()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Stop rollback because there is no data change " +
                            "between the before data snapshot and the current data snapshot.");
                }
                // no need continue undo.
                // 如果当前值与前镜像相同，则不回滚
                return false;
            } else {
                if (LOGGER.isInfoEnabled()) {
                    if (StringUtils.isNotBlank(afterEqualsCurrentResult.getErrMsg())) {
                        LOGGER.info(afterEqualsCurrentResult.getErrMsg(), afterEqualsCurrentResult.getErrMsgParams());
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("check dirty data failed, old and new data are not equal, " +
                            "tableName:[" + sqlUndoLog.getTableName() + "]," +
                            "oldRows:[" + JSON.toJSONString(afterRecords.getRows()) + "]," +
                            "newRows:[" + JSON.toJSONString(currentRecords.getRows()) + "].");
                }
                // 如果当前值与前后镜像都不相同，则出现了"脏写"
                throw new SQLUndoDirtyException("Has dirty records when undo.");
            }
        }
        return true;
    }

    /**
     * Query current records.
     *
     * @param connectionProxy the connection proxy
     * @return the table records
     * @throws SQLException the sql exception
     */
    protected TableRecords queryCurrentRecords(ConnectionProxy connectionProxy) throws SQLException {
        Connection conn = connectionProxy.getTargetConnection();
        TableRecords undoRecords = getUndoRows();
        TableMeta tableMeta = undoRecords.getTableMeta();
        //the order of element matters
        List<String> pkNameList = tableMeta.getPrimaryKeyOnlyName();

        // pares pk values
        Map<String, List<Field>> pkRowValues = parsePkValues(getUndoRows());
        if (pkRowValues.size() == 0) {
            return TableRecords.empty(tableMeta);
        }
        // build check sql
        String firstKey = pkRowValues.keySet().stream().findFirst().get();
        int pkRowSize = pkRowValues.get(firstKey).size();
        String checkSQL = buildCheckSql(sqlUndoLog.getTableName(),
                SqlGenerateUtils.buildWhereConditionByPKs(pkNameList, pkRowSize, connectionProxy.getDbType()));

        PreparedStatement statement = null;
        ResultSet checkSet = null;
        TableRecords currentRecords;
        try {
            statement = conn.prepareStatement(checkSQL);
            int paramIndex = 1;
            int rowSize = pkRowValues.get(pkNameList.get(0)).size();
            for (int r = 0; r < rowSize; r++) {
                for (int c = 0; c < pkNameList.size(); c++) {
                    List<Field> pkColumnValueList = pkRowValues.get(pkNameList.get(c));
                    Field field = pkColumnValueList.get(r);
                    int dataType = tableMeta.getColumnMeta(field.getName()).getDataType();
                    statement.setObject(paramIndex, field.getValue(), dataType);
                    paramIndex++;
                }
            }

            checkSet = statement.executeQuery();
            currentRecords = TableRecords.buildRecords(tableMeta, checkSet);
        } finally {
            IOUtil.close(checkSet, statement);
        }
        return currentRecords;
    }

    /**
     * build sql for query current records.
     *
     * @param tableName the tableName to query
     * @param whereCondition the where condition
     * @return the check sql for query current records
     */
    protected String buildCheckSql(String tableName, String whereCondition) {
        return String.format(CHECK_SQL_TEMPLATE, tableName, whereCondition);
    }

    protected List<Field> getOrderedPkList(TableRecords image, Row row, String dbType) {
        List<Field> pkFields = new ArrayList<>();
        // To ensure the order of the pk, the order should based on getPrimaryKeyOnlyName.
        List<String> pkColumnNameListByOrder = image.getTableMeta().getPrimaryKeyOnlyName();
        List<String> pkColumnNameListNoOrder = row.primaryKeys()
                .stream()
                .map(e -> ColumnUtils.delEscape(e.getName(), dbType))
                .collect(Collectors.toList());
        pkColumnNameListByOrder.forEach(pkName -> {
            int pkIndex = pkColumnNameListNoOrder.indexOf(pkName);
            if (pkIndex != -1) {
                // add PK to the last of the list.
                pkFields.add(row.primaryKeys().get(pkIndex));
            }
        });
        return pkFields;
    }


    /**
     * Parse pk values Field List.
     *
     * @param records the records
     * @return each element represents a row. And inside a row list contains pk columns(Field).
     */
    protected Map<String, List<Field>> parsePkValues(TableRecords records) {
        return parsePkValues(records.getRows(), records.getTableMeta().getPrimaryKeyOnlyName());
    }

    /**
     * Parse pk values Field List.
     *
     * @param rows       pk rows
     * @param pkNameList pk column name
     * @return each element represents a row. And inside a row list contains pk columns(Field).
     */
    protected Map<String, List<Field>> parsePkValues(List<Row> rows, List<String> pkNameList) {
        List<Field> pkFieldList = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            List<Field> fields = rows.get(i).getFields();
            if (fields != null) {
                for (Field field : fields) {
                    if (pkNameList.stream().anyMatch(e -> field.getName().equalsIgnoreCase(e))) {
                        pkFieldList.add(field);
                    }
                }
            }
        }
        Map<String, List<Field>> pkValueMap = pkFieldList.stream().collect(Collectors.groupingBy(Field::getName));
        return pkValueMap;
    }

}
