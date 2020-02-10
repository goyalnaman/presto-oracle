/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.driver.OracleDriver;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * Implementation of OracleClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 *
 * @author Marcelo Paes Rech
 */
public class OracleClient
		extends BaseJdbcClient
{
	@Inject
	public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
						OracleConfig oracleConfig) throws SQLException
	{
		super(connectorId, config, "`", connectionFactory(config, oracleConfig));
	}

	private static ConnectionFactory connectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig)
			throws SQLException
	{
		Properties connectionProperties = basicConnectionProperties(config);
		connectionProperties.setProperty("useInformationSchema", "true");
		connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
		connectionProperties.setProperty("useUnicode", "true");
		connectionProperties.setProperty("characterEncoding", "utf8");
		connectionProperties.setProperty("tinyInt1isBit", "false");
		if (oracleConfig.isAutoReconnect()) {
			connectionProperties.setProperty("autoReconnect", String.valueOf(oracleConfig.isAutoReconnect()));
			connectionProperties.setProperty("maxReconnects", String.valueOf(oracleConfig.getMaxReconnects()));
		}
		if (oracleConfig.getConnectionTimeout() != null) {
			connectionProperties.setProperty("connectTimeout", String.valueOf(oracleConfig.getConnectionTimeout().toMillis()));
		}

		return new DriverConnectionFactory(new OracleDriver(), config.getConnectionUrl(), connectionProperties);
	}

	@Override
	public Set<String> getSchemaNames()
	{
		try (Connection connection = connectionFactory.openConnection();
			 ResultSet resultSet = connection.getMetaData().getSchemas()) {
			ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
			while (resultSet.next()) {
				String schemaName = resultSet.getString(1).toLowerCase();
				schemaNames.add(schemaName);
			}
			return schemaNames.build();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void abortReadConnection(Connection connection)
			throws SQLException
	{
		connection.abort(directExecutor());
	}

	protected ResultSet getTables(Connection connection, String schemaName,
								  String tableName) throws SQLException
	{
		// Here we put TABLE and SYNONYM when the table schema is another user schema
		return connection.getMetaData().getTables(null, schemaName, tableName,
				new String[]{"TABLE", "SYNONYM"});
	}

	@Override
	public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
	{
		try (Connection connection = connectionFactory.openConnection()) {
			DatabaseMetaData metadata = connection.getMetaData();
			String jdbcSchemaName = schemaTableName.getSchemaName();
			String jdbcTableName = schemaTableName.getTableName();
			if (metadata.storesUpperCaseIdentifiers()) {
				jdbcSchemaName = jdbcSchemaName.toUpperCase();
				jdbcTableName = jdbcTableName.toUpperCase();
			}
			try (ResultSet resultSet = getTables(connection, jdbcSchemaName,
					jdbcTableName)) {
				List<JdbcTableHandle> tableHandles = new ArrayList<>();
				while (resultSet.next()) {
					tableHandles.add(new JdbcTableHandle(connectorId,
							schemaTableName, resultSet.getString("TABLE_CAT"),
							resultSet.getString("TABLE_SCHEMA"), resultSet
							.getString("TABLE_NAME")));
				}
				if (tableHandles.isEmpty()) {
					return null;
				}
				if (tableHandles.size() > 1) {
					throw new PrestoException(NOT_SUPPORTED,
							"Multiple tables matched: " + schemaTableName);
				}
				return getOnlyElement(tableHandles);
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
	{
		try (Connection connection = connectionFactory.openConnection()) {
			//If the table is mapped to another user you will need to get the synonym to that table
			//So, in this case, is mandatory to use setIncludeSynonyms
			((oracle.jdbc.driver.OracleConnection) connection).setIncludeSynonyms(true);
			DatabaseMetaData metadata = connection.getMetaData();
			String schemaName = tableHandle.getSchemaName().toUpperCase();
			String tableName = tableHandle.getTableName().toUpperCase();
			try (ResultSet resultSet = metadata.getColumns(null, schemaName,
					tableName, null)) {
				List<JdbcColumnHandle> columns = new ArrayList<>();
				boolean found = false;
				while (resultSet.next()) {
					found = true;
					JdbcTypeHandle jdbcTypeHandle = new JdbcTypeHandle(resultSet.getInt("DATA_TYPE"),
							resultSet.getInt("COLUMN_SIZE"),
							resultSet.getInt("DECIMAL_DIGITS"));
					Optional<ReadMapping> columnType = toPrestoType(session, jdbcTypeHandle);

					// skip unsupported column types
					if (columnType.isPresent()) {
						String columnName = resultSet.getString("COLUMN_NAME");
						columns.add(new JdbcColumnHandle(connectorId,
								columnName, jdbcTypeHandle, columnType.get().getType()));
					}
				}
				if (!found) {
					throw new TableNotFoundException(
							tableHandle.getSchemaTableName());
				}
				if (columns.isEmpty()) {
					throw new PrestoException(NOT_SUPPORTED,
							"Table has no supported column types: "
									+ tableHandle.getSchemaTableName());
				}
				return ImmutableList.copyOf(columns);
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<SchemaTableName> getTableNames(String schema)
	{
		try (Connection connection = connectionFactory.openConnection()) {
			DatabaseMetaData metadata = connection.getMetaData();
			if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
				schema = schema.toUpperCase();
			}
			try (ResultSet resultSet = getTables(connection, schema, null)) {
				ImmutableList.Builder<SchemaTableName> list = ImmutableList
						.builder();
				while (resultSet.next()) {
					list.add(getSchemaTableName(resultSet));
				}
				return list.build();
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected SchemaTableName getSchemaTableName(ResultSet resultSet)
			throws SQLException
	{
		String tableSchema = resultSet.getString("TABLE_SCHEMA");
		String tableName = resultSet.getString("TABLE_NAME");
		if (tableSchema != null) {
			tableSchema = tableSchema.toLowerCase();
		}
		if (tableName != null) {
			tableName = tableName.toLowerCase();
		}
		return new SchemaTableName(tableSchema, tableName);
	}

	@Override
	protected String toSqlType(Type type)
	{
		//just for debug
		String sqlType = super.toSqlType(type);
		return sqlType;
	}
}
