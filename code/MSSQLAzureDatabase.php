<?php
/**
 * Specific support for SQL Azure databases running on Windows Azure.
 * Currently only supports the SQLSRV driver from Microsoft.
 * 
 * Some important things about SQL Azure:
 * 
 * Selecting a database is not supported.
 * In order to change the database currently in use, you need to connect to
 * the database using the "Database" parameter with sqlsrv_connect()
 * 
 * Multiple active result sets are not supported. This means you can't
 * have two query results open at once.
 * 
 * Fulltext indexes are not supported.
 * 
 * @author Sean Harvey <sean at silverstripe dot com>
 * @package mssql
 */
class MSSQLAzureDatabase extends MSSQLDatabase {

	protected $fullTextEnabled = false;

	public function __construct($parameters) {
		$this->connectDatabase($parameters);
	}

	/**
	 * Connect to a SQL Azure database with the given parameters.
	 * @param array $parameters Connection parameters set by environment
	 * @return resource SQL Azure database connection link
	 */
	protected function connectDatabase($parameters) {
		$this->dbConn = sqlsrv_connect($parameters['server'], array(
			'Database' => $parameters['database'],
			'UID' => $parameters['username'],
			'PWD' => $parameters['password'],
			'MultipleActiveResultSets' => '0'
		));

		$this->tableList = $this->fieldList = $this->indexList = null;
		$this->database = $parameters['database'];
		$this->active = true;
		$this->fullTextEnabled = false;

		$this->query('SET QUOTED_IDENTIFIER ON');
		$this->query('SET TEXTSIZE 2147483647');
	}

	/**
	 * Switches to the given database.
	 * 
	 * If the database doesn't exist, you should call
	 * createDatabase() after calling selectDatabase()
	 *
	 * IMPORTANT: SQL Azure doesn't support "USE", so we need
	 * to reinitialize the database connection with the requested
	 * database name.
	 * 
	 * @param string $dbname The database name to switch to
	 */
	public function selectDatabase($dbname) {
		global $databaseConfig;
		$parameters = $databaseConfig;
		$parameters['database'] = $dbname;
		$this->connectDatabase($parameters);
	}

}