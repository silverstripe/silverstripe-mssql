<?php
/**
 * Specific support for SQL Azure databases running on Windows Azure.
 * "sqlsrv" for PHP MUST be installed to use SQL Azure. It does not support
 * the mssql_*() functions in PHP, as SQL Azure is Windows only.
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
		$this->dbConn = $this->connectDatabase($parameters);
	}

	/**
	 * Connect to a SQL Azure database with the given parameters.
	 * @param array $parameters Connection parameters set by environment
	 * @return resource SQL Azure database connection link
	 */
	protected function connectDatabase($parameters) {
		$conn = sqlsrv_connect($parameters['server'], array(
			'Database' => $parameters['database'],
			'UID' => $parameters['username'],
			'PWD' => $parameters['password'],
			'MultipleActiveResultSets' => '0'
		));

		$this->tableList = $this->fieldList = $this->indexList = null;
		$this->database = $parameters['database'];
		$this->active = true;
		$this->mssql = false; // mssql functions don't work with this database
		$this->fullTextEnabled = false;

		$this->query('SET QUOTED_IDENTIFIER ON');
		$this->query('SET TEXTSIZE 2147483647');
		
		return $conn
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
		$parameters = array();
		$parameters['database'] = $dbname;
		$parameters['server'] = $databaseConfig['server'];
		$parameters['username'] = $databaseConfig['username'];
		$parameters['password'] = $databaseConfig['password'];
		$this->connectDatabase($parameters);
	}

}