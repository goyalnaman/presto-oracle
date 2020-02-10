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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

/**
 * To get the custom properties to connect to the database. User, password and
 * URL is provided by de BaseJdbcClient is not required. If there is another
 * custom configuration it should be put in here.
 *
 * @author Marcelo Paes Rech
 */
public class OracleConfig
{
	private boolean autoReconnect = true;
	private int maxReconnects = 3;
	private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
	private String user;
	private String password;
	private String url;

	public boolean isAutoReconnect()
	{
		return autoReconnect;
	}

	@Config("oracle.auto-reconnect")
	public OracleConfig setAutoReconnect(boolean autoReconnect)
	{
		this.autoReconnect = autoReconnect;
		return this;
	}

	@Min(1)
	public int getMaxReconnects()
	{
		return maxReconnects;
	}

	@Config("oracle.max-reconnects")
	public OracleConfig setMaxReconnects(int maxReconnects)
	{
		this.maxReconnects = maxReconnects;
		return this;
	}

	public Duration getConnectionTimeout()
	{
		return connectionTimeout;
	}

	@Config("oracle.connection-timeout")
	public OracleConfig setConnectionTimeout(Duration connectionTimeout)
	{
		this.connectionTimeout = connectionTimeout;
		return this;
	}

	/**
	 * @return the user
	 */
	public String getUser()
	{
		return user;
	}

	/**
	 * @param user the user to set
	 */
	@Config("oracle.user")
	public OracleConfig setUser(String user)
	{
		this.user = user;
		return this;
	}

	/**
	 * @return the password
	 */
	public String getPassword()
	{
		return password;
	}

	/**
	 * @param password the password to set
	 */
	@Config("oracle.password")
	public OracleConfig setPassword(String password)
	{
		this.password = password;
		return this;
	}

	/**
	 * @return the url
	 */
	public String getUrl()
	{
		return url;
	}

	/**
	 * @param url the url to set
	 */
	@Config("oracle.url")
	public OracleConfig setUrl(String url)
	{
		this.url = url;
		return this;
	}
}
