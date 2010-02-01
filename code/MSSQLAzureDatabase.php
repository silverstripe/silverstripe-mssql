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
		$this->mssql = false;

		$connectionInfo = array(
			'Database' => $parameters['database'],
			'UID' => $parameters['username'],
			'PWD' => $parameters['password'],
			'MultipleActiveResultSets' => '0'
		);

		$this->dbConn = sqlsrv_connect($parameters['server'], $connectionInfo);

		if(!$this->dbConn) {
			$this->databaseError("Couldn't connect to MS SQL database");
		} else {
			$this->database = $parameters['database'];
			$this->active = true;
			$this->fullTextEnabled = false;
				
			// Configure the connection
			$this->query('SET QUOTED_IDENTIFIER ON');
			$this->query('SET TEXTSIZE 2147483647');
		}
	}

}