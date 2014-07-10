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
	
	/**
	 * List of parameters used to create new Azure connections between databases
	 * 
	 * @var array
	 */
	protected $parameters = array();

	public function fullTextEnabled() {
		return false;
	}

	public function __construct($parameters) {
		$this->connectDatabase($parameters);
	}

	/**
	 * Connect to a SQL Azure database with the given parameters.
	 * @param array $parameters Connection parameters set by environment
	 *  - server: The server, eg, localhost
	 *  - username: The username to log on with
	 *  - password: The password to log on with
	 *  - database: The database to connect to
	 *  - windowsauthentication: Not supported for Azure
	 */
	protected function connect($parameters) {
		$this->parameters = $parameters;
		$this->connectDatabase($parameters['database']);
	}
	
	/**
	 * Connect to a database using the provided parameters
	 * 
	 * @param string $database
	 */
	protected function connectDatabase($database) {
		$parameters = $this->parameters;
		$parameters['database'] = $database;
		$parameters['multipleactiveresultsets'] = 0;

		// Ensure that driver is available (required by PDO)
		if(empty($parameters['driver'])) {
			$parameters['driver'] = $this->getDatabaseServer();
		}

		// Notify connector of parameters, instructing the connector
		// to connect immediately to the Azure database
		$this->connector->connect($parameters, true);

		// Configure the connection
		$this->query('SET QUOTED_IDENTIFIER ON');
		$this->query('SET TEXTSIZE 2147483647');
	}

	/**
	 * Switches to the given database.
	 *
	 * IMPORTANT: SQL Azure doesn't support "USE", so we need
	 * to reinitialize the database connection with the requested
	 * database name.
	 * @see http://msdn.microsoft.com/en-us/library/windowsazure/ee336288.aspx
	 * 
	 * @param type $name The database name to switch to
	 * @param type $create
	 * @param type $errorLevel
	 */
	public function selectDatabase($name, $create = false, $errorLevel = E_USER_ERROR) {
		$this->fullTextEnabled = null;
		if (!$this->schemaManager->databaseExists($name)) {
			// Check DB creation permisson
			if (!$create) {
				if ($errorLevel !== false) {
					user_error("Attempted to connect to non-existing database \"$name\"", $errorLevel);
				}
				// Unselect database
				$this->connector->unloadDatabase();
				return false;
			}
			$this->schemaManager->createDatabase($name);
		}
		$this->connectDatabase($name);
		return true;
	}
}
