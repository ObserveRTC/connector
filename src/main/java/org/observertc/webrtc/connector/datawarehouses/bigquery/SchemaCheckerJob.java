/*
 * Copyright  2020 Balazs Kreith
 *
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

package org.observertc.webrtc.connector.datawarehouses.bigquery;

import com.google.cloud.bigquery.BigQuery;
import org.observertc.webrtc.connector.adapters.Job;
import org.observertc.webrtc.connector.adapters.Task;
import org.observertc.webrtc.connector.adapters.bigquery.prehistoric.Config;
import org.observertc.webrtc.connector.adapters.bigquery.prehistoric.CreateTables;
import org.observertc.webrtc.connector.sinks.bigquery.models.EntryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SchemaCheckerJob extends Job {

	private static final Logger logger = LoggerFactory.getLogger(SchemaCheckerJob.class);
	private static final String VERSION_01_JOB_NAME = "Version01SchemaCheck";

	private static volatile boolean run = false;

	private final BigQuery bigQuery;
	private String projectId;
	private String datasetId;
	private final Map<EntryType, String> tableNames;
	private boolean createDatasetIfNotExists = false;
	private boolean createTableIfNotExists = false;
	private String deleteTableIfExists = null;

	public SchemaCheckerJob(BigQuery bigQuery) {
		this.tableNames = new HashMap<>();
		this.bigQuery = bigQuery;
	}



	public SchemaCheckerJob withCreateDatasetIfNotExists(boolean value) {
		this.createDatasetIfNotExists = value;
		return this;
	}

	public SchemaCheckerJob withProjectId(String value) {
		this.projectId = value;
		return this;
	}

	public SchemaCheckerJob withDatasetId(String value) {
		this.datasetId = value;
		return this;
	}

	public SchemaCheckerJob withEntryName(EntryType entryName, String tableName) {
		this.tableNames.put(entryName, tableName);
		return this;
	}

	public SchemaCheckerJob withCreateTableIfNotExists(boolean value) {
		this.createTableIfNotExists = value;
		return this;
	}

	public SchemaCheckerJob withDeleteTableIfExists(String value) {
		this.deleteTableIfExists = value;
		return this;
	}

	@Override
	public void run() {
		Config config;
		config = new Config(
				this.bigQuery,
				this.projectId,
				this.datasetId,
				this.tableNames,
				this.createDatasetIfNotExists,
				this.createTableIfNotExists,
				this.deleteTableIfExists);
		Job createTables = new CreateTables(config);
		this.withTask(createTables);

		super.run();
	}

	private Task makeVersion1Check() {
		Config config;
		config = new Config(
				this.bigQuery,
				this.projectId,
				this.datasetId,
				this.tableNames,
				this.createDatasetIfNotExists,
				this.createTableIfNotExists,
				this.deleteTableIfExists);
		Job createTables = new CreateTables(config);

		return new Job(VERSION_01_JOB_NAME)
				.withTask(createTables);
	}

}
