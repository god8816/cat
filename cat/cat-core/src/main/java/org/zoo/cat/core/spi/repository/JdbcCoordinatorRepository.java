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

package org.zoo.cat.core.spi.repository;

import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariDataSource;
import org.zoo.cat.annotation.CatSPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoo.cat.common.bean.entity.CatNoticeSafe;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.config.CatDbConfig;
import org.zoo.cat.common.constant.CommonConstant;
import org.zoo.cat.common.enums.RepositorySupportEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.exception.CatRuntimeException;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.DbTypeUtils;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.common.utils.RepositoryPathUtils;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.core.helper.SqlHelper;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * jdbc impl.
 *
 * @author dzc
 */
@CatSPI("db")
public class JdbcCoordinatorRepository implements CatCoordinatorRepository {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCoordinatorRepository.class);

    private DataSource dataSource;

    private String tableName;

    private String currentDBType;

    private ObjectSerializer serializer;

    @Override
    public void setSerializer(final ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public int create(final CatTransaction catTransaction) {
        String sql = "insert into " + tableName + "(id,trans_id,trans_type,target_class,target_method,retry_max,retried_count,"
                + "create_time,last_time,version,status,invocation,role,pattern,confirm_method,cancel_method)"
                + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            final byte[] serialize = serializer.serialize(catTransaction.getCatParticipants());
            return executeUpdate(sql,catTransaction.getId(),catTransaction.getTransId(),catTransaction.getTransType(), catTransaction.getTargetClass(), catTransaction.getTargetMethod(),
            		    catTransaction.getRetryMax(),catTransaction.getRetriedCount(), catTransaction.getCreateTime(), catTransaction.getLastTime(),
                    catTransaction.getVersion(), catTransaction.getStatus(), serialize, catTransaction.getRole(),
                    catTransaction.getPattern(), catTransaction.getConfirmMethod(), catTransaction.getCancelMethod());
        } catch (CatException e) {
            e.printStackTrace();
            return FAIL_ROWS;
        }
    }

    @Override
    public int remove(final String id) {
        String sql = "delete from " + tableName + " where trans_id = ? ";
        return executeUpdate(sql, id);
    }

    @Override
    public int update(final CatTransaction catTransaction) {
        final Integer currentVersion = catTransaction.getVersion();
        catTransaction.setLastTime(new Date());
        catTransaction.setVersion(catTransaction.getVersion() + 1);
        String sql = "update " + tableName
                + " set last_time = ?,version =?,retried_count =?,invocation=?,status=? ,pattern=? where trans_id = ? and version=? ";
        try {
            final byte[] serialize = serializer.serialize(catTransaction.getCatParticipants());
            return executeUpdate(sql, catTransaction.getLastTime(),
                    catTransaction.getVersion(), catTransaction.getRetriedCount(), serialize,
                    catTransaction.getStatus(), catTransaction.getPattern(),
                    catTransaction.getTransId(), currentVersion);
        } catch (CatException e) {
            e.printStackTrace();
            return FAIL_ROWS;
        }
    }

    @Override
    public int updateParticipant(final CatTransaction catTransaction) {
        String sql = "update " + tableName + " set invocation=?  where trans_id = ?  ";
        try {
            final byte[] serialize = serializer.serialize(catTransaction.getCatParticipants());
            return executeUpdate(sql, serialize, catTransaction.getTransId());
        } catch (CatException e) {
            e.printStackTrace();
            return FAIL_ROWS;
        }
    }

    @Override
    public int updateStatus(final String id, final Integer status) {
        String sql = "update " + tableName + " set status=?  where trans_id = ?  ";
        return executeUpdate(sql, status, id);
    }

    @Override
    public CatTransaction findById(final String id) {
        String selectSql = "select * from " + tableName + " where trans_id=?";
        List<Map<String, Object>> list = executeQuery(selectSql, id);
        if (CollectionUtils.isNotEmpty(list)) {
            return list.stream()
                    .filter(Objects::nonNull)
                    .map(this::buildByResultMap)
                    .findFirst().orElse(null);
        }
        return null;
    }

    @Override
    public List<CatTransaction> listAll() {
        String selectSql = "select * from " + tableName;
        List<Map<String, Object>> list = executeQuery(selectSql);
        if (CollectionUtils.isNotEmpty(list)) {
            return list.stream()
                    .filter(Objects::nonNull)
                    .map(this::buildByResultMap)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public List<CatTransaction> listAllByDelay(final Date date) {
        String sb = "select * from " + tableName + " where last_time <? and retried_count<retry_max";
        List<Map<String, Object>> list = executeQuery(sb, date);
        if (CollectionUtils.isNotEmpty(list)) {
            return list.stream().filter(Objects::nonNull)
                    .map(this::buildByResultMap)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private CatTransaction buildByResultMap(final Map<String, Object> map) {
        CatTransaction catTransaction = new CatTransaction();
        catTransaction.setId((Long) map.get("id"));
        catTransaction.setTransId((String) map.get("trans_id"));
        catTransaction.setTransType((String) map.get("trans_type"));
        catTransaction.setRetryMax((Integer) map.get("retry_max"));
        catTransaction.setRetriedCount((Integer) map.get("retried_count"));
        catTransaction.setCreateTime((Date) map.get("create_time"));
        catTransaction.setLastTime((Date) map.get("last_time"));
        catTransaction.setVersion((Integer) map.get("version"));
        catTransaction.setStatus((Integer) map.get("status"));
        catTransaction.setRole((Integer) map.get("role"));
        catTransaction.setPattern((Integer) map.get("pattern"));
        byte[] bytes = (byte[]) map.get("invocation");
        try {
            final List<CatParticipant> catParticipants = serializer.deSerialize(bytes, CopyOnWriteArrayList.class);
            catTransaction.setCatParticipants(catParticipants);
        } catch (CatException e) {
            e.printStackTrace();
        }catch (Exception e) {
            //修改重试次数到最大重试次数
	    	    if(Objects.nonNull(e.getCause()) && Objects.nonNull(catTransaction) && e.getCause() instanceof ClassNotFoundException) {
	    	       	   catTransaction.setRetriedCount(catTransaction.getRetryMax());
	               update(catTransaction);
	    	    }
	    	    LogUtil.error(LOGGER, "反序列化日志异常，修改重试次数到最大:{}", e::getMessage);
	    	    e.printStackTrace();
		}
        return catTransaction;
    }

    @Override
    public void init(final String modelName, final CatConfig txConfig) {
        try {
            final CatDbConfig catDbConfig = txConfig.getCatDbConfig();
            if (catDbConfig.getDataSource() != null && StringUtils.isBlank(catDbConfig.getUrl())) {
                dataSource = catDbConfig.getDataSource();
            } else {
                HikariDataSource hikariDataSource = new HikariDataSource();
                hikariDataSource.setJdbcUrl(catDbConfig.getUrl());
                hikariDataSource.setDriverClassName(catDbConfig.getDriverClassName());
                hikariDataSource.setUsername(catDbConfig.getUsername());
                hikariDataSource.setPassword(catDbConfig.getPassword());
                hikariDataSource.setMaximumPoolSize(catDbConfig.getMaxActive());
                hikariDataSource.setMinimumIdle(catDbConfig.getMinIdle());
                hikariDataSource.setConnectionTimeout(catDbConfig.getConnectionTimeout());
                hikariDataSource.setIdleTimeout(catDbConfig.getIdleTimeout());
                hikariDataSource.setMaxLifetime(catDbConfig.getMaxLifetime());
                hikariDataSource.setConnectionTestQuery(catDbConfig.getConnectionTestQuery());
                if (catDbConfig.getDataSourcePropertyMap() != null && !catDbConfig.getDataSourcePropertyMap().isEmpty()) {
                    catDbConfig.getDataSourcePropertyMap().forEach(hikariDataSource::addDataSourceProperty);
                }
                dataSource = hikariDataSource;
            }
            this.tableName = RepositoryPathUtils.buildDbTableName(modelName);
            //save current database type
            this.currentDBType = DbTypeUtils.buildByDriverClassName(catDbConfig.getDriverClassName());
            executeUpdate(SqlHelper.buildCreateTableSql(catDbConfig.getDriverClassName(), tableName));
        } catch (Exception e) {
            LogUtil.error(LOGGER, "cat jdbc log init exception please check config:{}", e::getMessage);
            throw new CatRuntimeException(e);
        }
    }

    @Override
    public String getScheme() {
        return RepositorySupportEnum.DB.getSupport();
    }

    private int executeUpdate(final String sql, final Object... params) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            ps = connection.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, convertDataTypeToDB(params[i]));
                }
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("executeUpdate-> " + e.getMessage());
            return FAIL_ROWS;
        } finally {
            close(connection, ps, null);
        }

    }

    private Object convertDataTypeToDB(final Object params) {
        //https://jdbc.postgresql.org/documentation/head/8-date-time.html
        if (CommonConstant.DB_POSTGRESQL.equals(currentDBType) && params instanceof java.util.Date) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Date) params).getTime()), ZoneId.systemDefault());
        }
        return params;
    }

    private List<Map<String, Object>> executeQuery(final String sql, final Object... params) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = null;
        try {
            connection = dataSource.getConnection();
            ps = connection.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, convertDataTypeToDB(params[i]));
                }
            }
            rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> rowData = Maps.newHashMap();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(rowData);
            }
        } catch (SQLException e) {
            LOGGER.error("executeQuery-> " + e.getMessage());
        } finally {
            close(connection, ps, rs);
        }
        return list;
    }

    private void close(final Connection con, final PreparedStatement ps, final ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        try {
            if (con != null) {
                con.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

    }

	@Override
	public List<CatNoticeSafe> countLogsByDelay(Date date,String timeUnit) {
		String sb = "select  count(id) as num, target_method,target_class from cat_undo_log  where trans_type='notice' and create_time > ? group by target_method,target_class"; 
        List<Map<String, Object>> list = executeQuery(sb, date);
        if (CollectionUtils.isNotEmpty(list)) {
             List<CatNoticeSafe> catNoticeSafeList = new ArrayList<>();
             list.stream().filter(Objects::nonNull)
                    .forEach(item -> {
                      	        CatNoticeSafe catNoticeSafe = CatNoticeSafe.builder()
                                .num((Long)item.get("num"))
                                .targetMethod((String)item.get("target_method"))
                                .targetClass((String)item.get("target_class"))
                                .timeUnit(timeUnit)
                                .build();
                      	      catNoticeSafeList.add(catNoticeSafe);
                    });
                    return catNoticeSafeList;
        }
        return Collections.emptyList();
	}

	@Override
	public int removeLogsByDelay(Date acquireSecondsData) {
		 String sql = "delete from " + tableName + " where create_time <= ? ";
	     return executeUpdate(sql, acquireSecondsData);
	}
	
}
