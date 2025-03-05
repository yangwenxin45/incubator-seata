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
import org.apache.seata.rm.datasource.sql.struct.Field;
import org.apache.seata.rm.datasource.sql.struct.Row;
import org.apache.seata.rm.datasource.sql.struct.TableRecords;
import org.apache.seata.rm.datasource.undo.AbstractUndoExecutor;
import org.apache.seata.rm.datasource.undo.SQLUndoLog;
import org.apache.seata.sqlparser.util.ColumnUtils;
import org.apache.seata.sqlparser.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type My sql undo delete executor.
 *
 */
public class MySQLUndoDeleteExecutor extends AbstractUndoExecutor {

    /**
     * Instantiates a new My sql undo delete executor.
     *
     * @param sqlUndoLog the sql undo log
     */
    public MySQLUndoDeleteExecutor(SQLUndoLog sqlUndoLog) {
        super(sqlUndoLog);
    }

    /**
     * INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
     */
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

    /**
     * Undo delete.
     *
     * Notice: PK is at last one.
     * @see AbstractUndoExecutor#undoPrepare
     *
     * @return sql
     */
    @Override
    protected String buildUndoSQL() {
        // 获取前镜像
        TableRecords beforeImage = sqlUndoLog.getBeforeImage();
        // 获取前镜像的所有行
        List<Row> beforeImageRows = beforeImage.getRows();
        // 如果前镜像没有任何行，则表明 delete 语句没有删除任何行，抛出异常
        if (CollectionUtils.isEmpty(beforeImageRows)) {
            throw new ShouldNeverHappenException("Invalid UNDO LOG");
        }
        // 取出第一行
        Row row = beforeImageRows.get(0);
        List<Field> fields = new ArrayList<>(row.nonPrimaryKeys());
        fields.addAll(getOrderedPkList(beforeImage,row,JdbcConstants.MYSQL));

        // delete sql undo log before image all field come from table meta, need add escape.
        // see BaseTransactionalExecutor#buildTableRecords
        // 待插入的所有列名
        String insertColumns = fields.stream()
            .map(field -> ColumnUtils.addEscape(field.getName(), JdbcConstants.MYSQL))
            .collect(Collectors.joining(", "));
        // 待插入的值，用问号占位
        String insertValues = fields.stream().map(field -> MySQLJsonHelper.convertIfJson(field, beforeImage.getTableMeta()))
            .collect(Collectors.joining(", "));

        // 合成一个完整的 insert 语句
        return String.format(INSERT_SQL_TEMPLATE, sqlUndoLog.getTableName(), insertColumns, insertValues);
    }

    @Override
    protected TableRecords getUndoRows() {
        return sqlUndoLog.getBeforeImage();
    }
}
