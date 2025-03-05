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
package org.apache.seata.rm.datasource.exec;

import org.apache.seata.common.exception.NotSupportYetException;
import org.apache.seata.common.loader.EnhancedServiceLoader;
import org.apache.seata.common.util.CollectionUtils;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.datasource.StatementProxy;
import org.apache.seata.rm.datasource.exec.mariadb.MariadbInsertOnDuplicateUpdateExecutor;
import org.apache.seata.rm.datasource.exec.mariadb.MariadbUpdateJoinExecutor;
import org.apache.seata.rm.datasource.exec.mysql.MySQLInsertOnDuplicateUpdateExecutor;
import org.apache.seata.rm.datasource.exec.mysql.MySQLUpdateJoinExecutor;
import org.apache.seata.rm.datasource.exec.polardbx.PolarDBXInsertOnDuplicateUpdateExecutor;
import org.apache.seata.rm.datasource.exec.polardbx.PolarDBXUpdateJoinExecutor;
import org.apache.seata.rm.datasource.exec.sqlserver.SqlServerDeleteExecutor;
import org.apache.seata.rm.datasource.exec.sqlserver.SqlServerSelectForUpdateExecutor;
import org.apache.seata.rm.datasource.exec.sqlserver.SqlServerUpdateExecutor;
import org.apache.seata.rm.datasource.sql.SQLVisitorFactory;
import org.apache.seata.sqlparser.SQLRecognizer;
import org.apache.seata.sqlparser.SQLType;
import org.apache.seata.sqlparser.util.JdbcConstants;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The type Execute template.
 */
public class ExecuteTemplate {

    /**
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        return execute(null, statementProxy, statementCallback, args);
    }

    /**
     * 如果这个 SQL 语句不在分布式事务中，并且也没有查询全局锁的要求，
     * 则不需要将其纳入 Seata 框架下进行处理，用原始的 Statement 直接处理即可；
     * 反之则需要根据 SQL 语句的类型选用不同的执行器来执行
     *
     * @author yangwenxin
     * @date 2025-03-03 16:32
     */
    /**
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param sqlRecognizers    the sql recognizer list
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(List<SQLRecognizer> sqlRecognizers,
                                                     StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        // 如果不检查全局锁并且不是 AT 分支
        if (!RootContext.requireGlobalLock() && BranchType.AT != RootContext.getBranchType()) {
            // Just work as original statement
            // 则执行目标 Statement
            return statementCallback.execute(statementProxy.getTargetStatement(), args);
        }

        // 数据库类型
        String dbType = statementProxy.getConnectionProxy().getDbType();
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            // 获取 sqlRecognizers
            sqlRecognizers = SQLVisitorFactory.get(
                    statementProxy.getTargetSQL(),
                    dbType);
        }
        Executor<T> executor;
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            // 生成简单执行器
            executor = new PlainExecutor<>(statementProxy, statementCallback);
        } else {
            if (sqlRecognizers.size() == 1) {
                SQLRecognizer sqlRecognizer = sqlRecognizers.get(0);
                switch (sqlRecognizer.getSQLType()) {
                    case INSERT:
                        // 加载 insert 执行器
                        executor = EnhancedServiceLoader.load(InsertExecutor.class, dbType,
                                    new Class[]{StatementProxy.class, StatementCallback.class, SQLRecognizer.class},
                                    new Object[]{statementProxy, statementCallback, sqlRecognizer});
                        break;
                    case UPDATE:
                        // 加载 update 执行器
                        if (JdbcConstants.SQLSERVER.equalsIgnoreCase(dbType)) {
                            executor = new SqlServerUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        } else {
                            executor = new UpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        }
                        break;
                    case DELETE:
                        // 加载 delete 执行器
                        if (JdbcConstants.SQLSERVER.equalsIgnoreCase(dbType)) {
                            executor = new SqlServerDeleteExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        } else {
                            executor = new DeleteExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        }
                        break;
                    case SELECT_FOR_UPDATE:
                        // 加载 select ... for update 执行器
                        if (JdbcConstants.SQLSERVER.equalsIgnoreCase(dbType)) {
                            executor = new SqlServerSelectForUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        } else {
                            executor = new SelectForUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        }
                        break;
                    case INSERT_ON_DUPLICATE_UPDATE:
                        switch (dbType) {
                            case JdbcConstants.MYSQL:
                                executor =
                                        new MySQLInsertOnDuplicateUpdateExecutor(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            case JdbcConstants.MARIADB:
                                executor =
                                        new MariadbInsertOnDuplicateUpdateExecutor(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            case JdbcConstants.POLARDBX:
                                executor = new PolarDBXInsertOnDuplicateUpdateExecutor(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            default:
                                throw new NotSupportYetException(dbType + " not support to INSERT_ON_DUPLICATE_UPDATE");
                        }
                        break;
                    case UPDATE_JOIN:
                        switch (dbType) {
                            case JdbcConstants.MYSQL:
                                executor = new MySQLUpdateJoinExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            case JdbcConstants.MARIADB:
                                executor = new MariadbUpdateJoinExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            case JdbcConstants.POLARDBX:
                                executor = new PolarDBXUpdateJoinExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                                break;
                            default:
                                throw new NotSupportYetException(dbType + " not support to " + SQLType.UPDATE_JOIN.name());
                        }
                        break;
                    default:
                        // 加载简单执行器
                        executor = new PlainExecutor<>(statementProxy, statementCallback);
                        break;
                }
            } else {
                // 多执行器
                executor = new MultiExecutor<>(statementProxy, statementCallback, sqlRecognizers);
            }
        }
        T rs;
        try {
            // 通过执行器执行
            rs = executor.execute(args);
        } catch (Throwable ex) {
            if (!(ex instanceof SQLException)) {
                // Turn other exception into SQLException
                ex = new SQLException(ex);
            }
            throw (SQLException) ex;
        }
        return rs;
    }

}
