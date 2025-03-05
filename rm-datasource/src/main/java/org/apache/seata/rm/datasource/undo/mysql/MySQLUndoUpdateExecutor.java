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
package org.apache.seata.rm.datasource.undo.mysql;

import org.apache.seata.common.exception.ShouldNeverHappenException;
import org.apache.seata.common.util.CollectionUtils;
import org.apache.seata.rm.datasource.SqlGenerateUtils;
import org.apache.seata.rm.datasource.sql.struct.Field;
import org.apache.seata.rm.datasource.sql.struct.Row;
import org.apache.seata.rm.datasource.sql.struct.TableRecords;
import org.apache.seata.rm.datasource.undo.AbstractUndoExecutor;
import org.apache.seata.rm.datasource.undo.SQLUndoLog;
import org.apache.seata.sqlparser.util.ColumnUtils;
import org.apache.seata.sqlparser.util.JdbcConstants;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The type My sql undo update executor.
 *
 */
public class MySQLUndoUpdateExecutor extends AbstractUndoExecutor {

    /**
     * UPDATE a SET x = ?, y = ?, z = ? WHERE pk1 = ? and pk2 = ?
     */
    private static final String UPDATE_SQL_TEMPLATE = "UPDATE %s SET %s WHERE %s ";

    /**
     * Undo Update.
     *
     * @return sql
     */
    @Override
    protected String buildUndoSQL() {
        // 获取前镜像
        TableRecords beforeImage = sqlUndoLog.getBeforeImage();
        // 获取前镜像的所有行
        List<Row> beforeImageRows = beforeImage.getRows();
        // 如果前镜像没有任何行，则表明 update 语句没有更改任何行，抛出异常
        if (CollectionUtils.isEmpty(beforeImageRows)) {
            throw new ShouldNeverHappenException("Invalid UNDO LOG"); // TODO
        }
        // 取出第一行
        Row row = beforeImageRows.get(0);

        List<Field> nonPkFields = row.nonPrimaryKeys();
        // update sql undo log before image all field come from table meta. need add escape.
        // see BaseTransactionalExecutor#buildTableRecords
        // 待更新的所有列名
        String updateColumns = nonPkFields.stream().map(
            field -> {
                String addEscape = ColumnUtils.addEscape(field.getName(), JdbcConstants.MYSQL);
                return addEscape + " = " + MySQLJsonHelper.convertIfJson(field, beforeImage.getTableMeta());
            })
            .collect(Collectors.joining(", "));

        // 主键名列表
        List<String> pkNameList = getOrderedPkList(beforeImage, row, JdbcConstants.MYSQL).stream().map(e -> e.getName())
            .collect(Collectors.toList());
        // 构建 where 子句
        String whereSql = SqlGenerateUtils.buildWhereConditionByPKs(pkNameList, JdbcConstants.MYSQL);

        // 合成一个完整的 update 语句
        return String.format(UPDATE_SQL_TEMPLATE, sqlUndoLog.getTableName(), updateColumns, whereSql);
    }

    /**
     * Instantiates a new My sql undo update executor.
     *
     * @param sqlUndoLog the sql undo log
     */
    public MySQLUndoUpdateExecutor(SQLUndoLog sqlUndoLog) {
        super(sqlUndoLog);
    }

    @Override
    protected TableRecords getUndoRows() {
        return sqlUndoLog.getBeforeImage();
    }
}
