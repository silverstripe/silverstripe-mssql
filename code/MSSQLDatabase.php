<?php
/**
 * Microsoft SQL Server 2008+ connector class.
 *
 * <h2>Connecting using Windows</h2>
 *
 * If you've got your website running on Windows, it's highly recommended you
 * use Microsoft SQL Server Driver for PHP "sqlsrv".
 *
 * A complete guide to installing a Windows IIS + PHP + SQL Server web stack can be
 * found here: http://doc.silverstripe.org/installation-on-windows-server-manual-iis
 *
 * @see http://sqlsrvphp.codeplex.com/
 *
 * <h2>Connecting using Linux or Mac OS X</h2>
 *
 * The following commands assume you used the default package manager
 * to install PHP with the operating system.
 *
 * Debian, and Ubuntu:
 * <code>apt-get install php5-sybase</code>
 *
 * Fedora, CentOS and RedHat:
 * <code>yum install php-mssql</code>
 *
 * Mac OS X (MacPorts):
 * <code>port install php5-mssql</code>
 *
 * These packages will install the mssql extension for PHP, as well
 * as FreeTDS, which will let you connect to SQL Server.
 *
 * More information available in the SilverStripe developer wiki:
 * @see http://doc.silverstripe.org/modules:mssql
 * @see http://doc.silverstripe.org/installation-on-windows-server-manual-iis
 *
 * References:
 * @see http://freetds.org
 *
 * @package mssql
 */
class MSSQLDatabase extends SS_Database {

	/**
	 * Connection to the DBMS.
	 * @var resource
	 */
	protected $dbConn;

	/**
	 * True if we are connected to a database.
	 * @var boolean
	 */
	protected $active;

	/**
	 * The name of the database.
	 * @var string
	 */
	protected $database;

	/**
	 * If true, use the mssql_... functions.
	 * If false use the sqlsrv_... functions
	 */
	protected $mssql = null;

	/**
	 * Stores the affected rows of the last query.
	 * Used by sqlsrv functions only, as sqlsrv_rows_affected
	 * accepts a result instead of a database handle.
	 */
	protected $lastAffectedRows;

	/**
	 * Words that will trigger an error if passed to a SQL Server fulltext search
	 */
	public static $noiseWords = array('about', '1', 'after', '2', 'all', 'also', '3', 'an', '4', 'and', '5', 'another', '6', 'any', '7', 'are', '8', 'as', '9', 'at', '0', 'be', '$', 'because', 'been', 'before', 'being', 'between', 'both', 'but', 'by', 'came', 'can', 'come', 'could', 'did', 'do', 'does', 'each', 'else', 'for', 'from', 'get', 'got', 'has', 'had', 'he', 'have', 'her', 'here', 'him', 'himself', 'his', 'how', 'if', 'in', 'into', 'is', 'it', 'its', 'just', 'like', 'make', 'many', 'me', 'might', 'more', 'most', 'much', 'must', 'my', 'never', 'no', 'now', 'of', 'on', 'only', 'or', 'other', 'our', 'out', 'over', 're', 'said', 'same', 'see', 'should', 'since', 'so', 'some', 'still', 'such', 'take', 'than', 'that', 'the', 'their', 'them', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'up', 'use', 'very', 'want', 'was', 'way', 'we', 'well', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'will', 'with', 'would', 'you', 'your', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z');

	/**
	 * Transactions will work with FreeTDS, but not entirely with sqlsrv driver on Windows with MARS enabled.
	 * TODO:
	 * - after the test fails with open transaction, the transaction should be rolled back,
	 *   otherwise other tests will break claiming that transaction is still open.
	 * - figure out SAVEPOINTS
	 * - READ ONLY transactions
	 */
	protected $supportsTransactions = true;

	/**
	 * Cached flag to determine if full-text is enabled. This is set by
	 * {@link MSSQLDatabase::fullTextEnabled()}
	 *
	 * @var boolean
	 */
	protected $fullTextEnabled = null;

	/**
	 * Stores per-request cached constraint checks that come from the database.
	 * @var array
	 */
	protected static $cached_checks = array();

	/**
	 * @ignore
	 */
	protected static $collation = null;

	/**
	 * Set the default collation of the MSSQL nvarchar fields that we create.
	 * We don't apply this to the database as a whole, so that we can use unicode collations.
	 */
	public static function set_collation($collation) {
		self::$collation = $collation;
	}

	/**
	 * Connect to a MS SQL database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - server: The server, eg, localhost
	 *  - username: The username to log on with
	 *  - password: The password to log on with
	 *  - database: The database to connect to
	 */
	public function __construct($parameters) {
		if(function_exists('mssql_connect')) {
			$this->mssql = true;
		} else if(function_exists('sqlsrv_connect')) {
			$this->mssql = false;
		} else {
			user_error("Neither the mssql_connect() nor the sqlsrv_connect() functions are available.  Please install the PHP native mssql module, or the Microsoft-provided sqlsrv module.", E_USER_ERROR);
		}

		if($this->mssql) {
			// Switch to utf8 connection charset
			ini_set('mssql.charset', 'utf8');
			$this->dbConn = mssql_connect($parameters['server'], $parameters['username'], $parameters['password'], true);
		} else {
			// Disable default warnings as errors behaviour for sqlsrv to keep it in line with mssql functions
			if(ini_get('sqlsrv.WarningsReturnAsErrors')) {
				ini_set('sqlsrv.WarningsReturnAsErrors', 'Off');
			}

			$options = array(
				'CharacterSet' => 'UTF-8',
				'MultipleActiveResultSets' => true
			);
			if(!(defined('MSSQL_USE_WINDOWS_AUTHENTICATION') && MSSQL_USE_WINDOWS_AUTHENTICATION == true)) {
				$options['UID'] = $parameters['username'];
				$options['PWD'] = $parameters['password'];
			}

			$this->dbConn = sqlsrv_connect($parameters['server'], $options);
		}

		if(!$this->dbConn) {
			$this->databaseError('Couldn\'t connect to SQL Server database');
		} else {
			$this->database = $parameters['database'];
			$this->selectDatabase($this->database);

			// Configure the connection
			$this->query('SET QUOTED_IDENTIFIER ON');
			$this->query('SET TEXTSIZE 2147483647');
		}
	}

	public function __destruct() {
		if(is_resource($this->dbConn)) {
			if($this->mssql) {
				mssql_close($this->dbConn);
			} else {
				sqlsrv_close($this->dbConn);
			}
		}
	}

	/**
	 * Checks whether the current SQL Server version has full-text
	 * support installed and full-text is enabled for this database.
	 *
	 * @return boolean
	 */
	public function fullTextEnabled() {
		if($this->fullTextEnabled === null) {
			$isInstalled = (boolean) DB::query("SELECT fulltextserviceproperty('isfulltextinstalled')")->value();
			$enabledForDb = (boolean) DB::query("
				SELECT is_fulltext_enabled
				FROM sys.databases
				WHERE name = '$this->database'
			")->value();
			$this->fullTextEnabled = (boolean) ($isInstalled && $enabledForDb);
		}
		return $this->fullTextEnabled;
	}

	/**
	 * Throw a database error
	 */
	function databaseError($message, $errorLevel = E_USER_ERROR) {
		if(!$this->mssql) {
			$errorMessages = array();
			$errors = sqlsrv_errors();
			if ($errors) foreach($errors as $error) {
				$errorMessages[] = $error['message'];
			}
			$message .= ": \n" . implode("; ",$errorMessages);
		}

		return parent::databaseError($message, $errorLevel);
	}

	/**
	 * This will set up the full text search capabilities.
	 */
	function createFullTextCatalog() {
		$result = $this->fullTextCatalogExists();
		if(!$result) $this->query('CREATE FULLTEXT CATALOG "ftCatalog" AS DEFAULT;');
	}

	/**
	 * Check that a fulltext catalog has been created yet.
	 * @return boolean
	 */
	public function fullTextCatalogExists() {
		return (bool) $this->query("SELECT name FROM sys.fulltext_catalogs WHERE name = 'ftCatalog';")->value();
	}

	/**
	 * Sleep until the catalog has been fully rebuilt. This is a busy wait designed for situations
	 * when you need to be sure the index is up to date - for example in unit tests.
	 *
	 * TODO: move this to Database class? Can we assume this will be useful for all databases?
	 * Also see the wrapper functions "waitUntilIndexingFinished" in SearchFormTest and TranslatableSearchFormTest
	 *
	 * @param int $maxWaitingTime Time in seconds to wait for the database.
	 */
	function waitUntilIndexingFinished($maxWaitingTime = 15) {
		if($this->fullTextEnabled()) {
			$this->query("EXEC sp_fulltext_catalog 'ftCatalog', 'Rebuild';");

			// Busy wait until it's done updating, but no longer than 15 seconds.
			$start = time();
			while(time()-$start<$maxWaitingTime) {
				$status = $this->query("EXEC sp_help_fulltext_catalogs 'ftCatalog';")->first();

				if (isset($status['STATUS']) && $status['STATUS']==0) {
					// Idle!
					break;
				}
				sleep(1);
			}
		}
	}

	/**
	 * Not implemented, needed for PDO
	 */
	public function getConnect($parameters) {
		return null;
	}

	/**
	 * Returns true if this database supports collations
	 * @return boolean
	 */
	public function supportsCollations() {
		return true;
	}

	public function supportsTimezoneOverride() {
		return true;
	}

	/**
	 * Get the version of MSSQL.
	 * @return string
	 */
	public function getVersion() {
		return trim($this->query("SELECT CONVERT(char(15), SERVERPROPERTY('ProductVersion'))")->value());
	}

	/**
	 * Get the database server, namely mssql.
	 * @return string
	 */
	public function getDatabaseServer() {
		return "mssql";
	}

	public function query($sql, $errorLevel = E_USER_ERROR) {
		if(isset($_REQUEST['previewwrite']) && in_array(strtolower(substr($sql,0,strpos($sql,' '))), array('insert','update','delete','replace'))) {
			Debug::message("Will execute: $sql");
			return;
		}

		if(isset($_REQUEST['showqueries'])) {
			$starttime = microtime(true);
		}

		$error = '';
		if($this->mssql) {
			$handle = mssql_query($sql, $this->dbConn);
			$error = mssql_get_last_message();
		} else {
			$handle = sqlsrv_query($this->dbConn, $sql);
			if($handle) $this->lastAffectedRows = sqlsrv_rows_affected($handle);
			if(function_exists('sqlsrv_errors')) {
				$errInfo = sqlsrv_errors();
				if($errInfo) {
					foreach($errInfo as $info) {
						$error .= implode(', ', array($info['SQLSTATE'], $info['code'], $info['message']));
					}
				}
			}
		}

		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			Debug::message("\n$sql\n{$endtime}ms\n", false);
		}

		if(!$handle && $errorLevel) $this->databaseError("Couldn't run query ($error): $sql", $errorLevel);
		return new MSSQLQuery($this, $handle, $this->mssql);
	}

	public function getGeneratedID($table) {
		return $this->query("SELECT IDENT_CURRENT('$table')")->value();
	}

	/**
	 * MSSQL stores the primary key column with an internal identifier,
	 * so a lookup needs to be done to determine it.
	 * 
	 * @param string $tableName Name of table with primary key column "ID"
	 * @return string Internal identifier for primary key
	 */
	function getPrimaryKey($tableName){
		$indexes=DB::query("EXEC sp_helpindex '$tableName';");
		$indexName = '';
		foreach($indexes as $index) {
			if($index['index_keys'] == 'ID') {
				$indexName = $index['index_name'];
				break;
			}
		}

		return $indexName;
	}

	function getIdentityColumn($tableName) {
		return $this->query("
			SELECT
				TABLE_NAME + '.' + COLUMN_NAME,
				TABLE_NAME
 			FROM
				INFORMATION_SCHEMA.COLUMNS
 			WHERE
				TABLE_SCHEMA = 'dbo' AND
				COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND
				TABLE_NAME = '$tableName'
		")->value();
	}

	public function isActive() {
		return $this->active ? true : false;
	}

	/**
	 * Create the database that is currently selected.
	 */
	public function createDatabase() {
		$this->query("CREATE DATABASE \"$this->database\"");
		$this->selectDatabase($this->database);
	}

	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabase() {
		$db = $this->database;
		$this->selectDatabase('master');
		$this->query("DROP DATABASE \"$db\"");
		$this->active = false;
	}

	/**
	 * Drop the given database name.
	 * Use with caution.
	 * @param string $name Database name to drop
	 */
	public function dropDatabaseByName($name) {
		$this->query("DROP DATABASE \"$name\"");
	}

	/**
	 * Returns the name of the currently selected database
	 */
	public function currentDatabase() {
		return $this->database;
	}

	/**
	 * Switches to the given database.
	 *
	 * If the database doesn't exist, you should call
	 * createDatabase() after calling selectDatabase()
	 *
	 * @param string $dbname The database name to switch to
	 */
	public function selectDatabase($dbname) {
		$this->database = $dbname;

		if($this->databaseExists($this->database)) {
			if($this->mssql) {
				if(mssql_select_db($this->database, $this->dbConn)) {
					$this->active = true;
				}
			} else {
				$this->query("USE \"$this->database\"");
				$this->active = true;
			}
		}

		$this->tableList = $this->fieldList = $this->indexList = $this->fullTextEnabled = null;
	}

	/**
	 * Check if the given database exists from {@link allDatabaseNames()}.
	 * @param string $name Name of database to check exists
	 * @return boolean
	 */
	public function databaseExists($name) {
		$databases = $this->allDatabaseNames();
		foreach($databases as $dbname) {
			if($dbname == $name) return true;
		}
		return false;
	}

	/**
	 * Return all databases names from the server.
	 * @return array
	 */
	public function allDatabaseNames() {
		return $this->query('SELECT NAME FROM sys.sysdatabases')->column();
	}

	/**
	 * Create a new table.
	 * @param $tableName The name of the table
	 * @param $fields A map of field names to field types
	 * @param $indexes A map of indexes
	 * @param $options An map of additional options.  The available keys are as follows:
	 *   - 'MSSQLDatabase'/'MySQLDatabase'/'PostgreSQLDatabase' - database-specific options such as "engine" for MySQL.
	 *   - 'temporary' - If true, then a temporary table will be created
	 * @return The table name generated.  This may be different from the table name, for example with temporary tables.
	 */
	public function createTable($tableName, $fields = null, $indexes = null, $options = null, $advancedOptions = null) {
		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) $fieldSchemas .= "\"$k\" $v,\n";

		// Temporary tables start with "#" in MSSQL-land
		if(!empty($options['temporary'])) {
			// Randomize the temp table name to avoid conflicts in the tempdb table which derived databases share
			$tableName = "#$tableName" . '-' . rand(1000000, 9999999);
		}

		$this->query("CREATE TABLE \"$tableName\" (
			$fieldSchemas
			primary key (\"ID\")
		);");

		//we need to generate indexes like this: CREATE INDEX IX_vault_to_export ON vault (to_export);
		//This needs to be done AFTER the table creation, so we can set up the fulltext indexes correctly
		if($indexes) foreach($indexes as $k => $v) {
			$indexSchemas .= $this->getIndexSqlDefinition($tableName, $k, $v) . "\n";
		}

		if($indexSchemas) $this->query($indexSchemas);

		return $tableName;
	}

	/**
	 * Alter a table's schema.
	 * @param $table The name of the table to alter
	 * @param $newFields New fields, a map of field name => field schema
	 * @param $newIndexes New indexes, a map of index name => index type
	 * @param $alteredFields Updated fields, a map of field name => field schema
	 * @param $alteredIndexes Updated indexes, a map of index name => index type
	 */
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions=null, $advancedOptions=null) {
		$alterList = array();
		$indexList = $this->indexList($tableName);

		// drop any fulltext indexes that exist on the table before altering the structure
		if($this->fullTextIndexExists($tableName)) {
			$alterList[] = "\nDROP FULLTEXT INDEX ON \"$tableName\";";
		}

		if($newFields) foreach($newFields as $k => $v) $alterList[] = "ALTER TABLE \"$tableName\" ADD \"$k\" $v";

		if($alteredFields) foreach($alteredFields as $k => $v) $alterList[] = $this->alterTableAlterColumn($tableName, $k, $v, $indexList);
		if($alteredIndexes) foreach($alteredIndexes as $k => $v) $alterList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
		if($newIndexes) foreach($newIndexes as $k => $v) $alterList[] = $this->getIndexSqlDefinition($tableName, $k, $v);

		if($alterList) {
			foreach($alterList as $alteration) {
				if($alteration != '') {
					$this->query($alteration);
				}
			}
		}
	}

	/**
	 * Given the table and column name, retrieve the constraint name for that column
	 * in the table.
	 */
	public function getConstraintName($tableName, $columnName) {
		return $this->query("
			SELECT CONSTRAINT_NAME
			FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
			WHERE TABLE_NAME = '$tableName' AND COLUMN_NAME = '$columnName'
		")->value();
	}

	/**
	 * Given a table and column name, return a check constraint clause for that column in
	 * the table.
	 * 
	 * This is an expensive query, so it is cached per-request and stored by table. The initial
	 * call for a table that has not been cached will query all columns and store that
	 * so subsequent calls are fast.
	 * 
	 * @param string $tableName Table name column resides in
	 * @param string $columnName Column name the constraint is for
	 * @return string The check string
	 */
	public function getConstraintCheckClause($tableName, $columnName) {
		if(isset(self::$cached_checks[$tableName])) {
			if(!isset(self::$cached_checks[$tableName][$columnName])) self::$cached_checks[$tableName][$columnName] = null;
			return self::$cached_checks[$tableName][$columnName];
		}

		$checks = array();
		foreach($this->query("
			SELECT CAST(CHECK_CLAUSE AS TEXT) AS CHECK_CLAUSE, COLUMN_NAME
			FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS CC
			INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME = CC.CONSTRAINT_NAME
			WHERE TABLE_NAME = '$tableName'
		") as $record) {
			$checks[$record['COLUMN_NAME']] = $record['CHECK_CLAUSE'];
		}

		self::$cached_checks[$tableName] = $checks;
		if(!isset(self::$cached_checks[$tableName][$columnName])) self::$cached_checks[$tableName][$columnName] = null;

		return self::$cached_checks[$tableName][$columnName];
	}

	/**
	 * Return the name of the default constraint applied to $tableName.$colName.
	 * Will return null if no such constraint exists
	 */
	protected function defaultConstraintName($tableName, $colName) {
		return $this->query("SELECT s.name --default name
			FROM sys.sysobjects s
			join sys.syscolumns c ON s.parent_obj = c.id
			WHERE s.xtype = 'd'
			and c.cdefault = s.id
			and parent_obj= OBJECT_ID('$tableName')
			and c.name = '$colName'")->value();
	}


	/**
	 * Get enum values from a constraint check clause.
	 * @param string $clause Check clause to parse values from
	 * @return array Enum values
	 */
	protected function enumValuesFromCheckClause($clause) {
		$segments = preg_split('/ +OR *\[/i', $clause);
		$constraints = array();
		foreach($segments as $segment) {
			$bits = preg_split('/ *= */', $segment);
			for($i = 1; $i < sizeof($bits); $i += 2) {
				array_unshift($constraints, substr(rtrim($bits[$i], ')'), 1, -1));

		}
		}
		return $constraints;
	}

	/*
	 * Creates an ALTER expression for a column in MS SQL
	 *
	 * @param $tableName Name of the table to be altered
	 * @param $colName   Name of the column to be altered
	 * @param $colSpec   String which contains conditions for a column
	 * @return string
	 */
	protected function alterTableAlterColumn($tableName, $colName, $colSpec, $indexList){

		// First, we split the column specifications into parts
		// TODO: this returns an empty array for the following string: int(11) not null auto_increment
		//		 on second thoughts, why is an auto_increment field being passed through?
		$pattern = '/^([\w()]+)\s?((?:not\s)?null)?\s?(default\s[\w\']+)?\s?(check\s?[\w()\'",\s]+)?$/i';
		$matches=Array();
		preg_match($pattern, $colSpec, $matches);

		// drop the index if it exists
		$alterCol='';
		$indexName = isset($indexList[$colName]['indexname']) ? $indexList[$colName]['indexname'] : null;
		if($indexName && $colName != 'ID') {
			$alterCol = "\nDROP INDEX \"$indexName\" ON \"$tableName\";";
		}

		$prefix="ALTER TABLE \"" . $tableName . "\" ";

		// Remove the old default prior to adjusting the column.
		if($defaultConstraintName = $this->defaultConstraintName($tableName, $colName)) {
			$alterCol .= ";\n$prefix DROP CONSTRAINT \"$defaultConstraintName\"";
		}

		if(isset($matches[1])) {
			//We will prevent any changes being made to the ID column.  Primary key indexes will have a fit if we do anything here.
			if($colName!='ID'){

				// SET null / not null
				if(!empty($matches[2])) $alterCol .= ";\n$prefix ALTER COLUMN \"$colName\" $matches[1] $matches[2]";
				else $alterCol .= ";\n$prefix ALTER COLUMN \"$colName\" $matches[1]";

				// Add a default back
				if(!empty($matches[3])) $alterCol .= ";\n$prefix ADD $matches[3] FOR \"$colName\"";

				// SET check constraint (The constraint HAS to be dropped)
				if(!empty($matches[4])) {
					$constraint = $this->getConstraintName($tableName, $colName);
					if($constraint) {
						$alterCol .= ";\n$prefix DROP CONSTRAINT {$constraint}";
					}

					//NOTE: 'with nocheck' seems to solve a few problems I've been having for modifying existing tables.
					$alterCol .= ";\n$prefix WITH NOCHECK ADD CONSTRAINT \"{$tableName}_{$colName}_check\" $matches[4]";
				}
			}
		}

		return isset($alterCol) ? $alterCol : '';
	}

	public function renameTable($oldTableName, $newTableName) {
		$this->query("EXEC sp_rename \"$oldTableName\", \"$newTableName\"");
	}

	/**
	 * Checks a table's integrity and repairs it if necessary.
	 * NOTE: MSSQL does not appear to support any vacuum or optimise commands
	 *
	 * @var string $tableName The name of the table.
	 * @return boolean Return true if the table has integrity after the method is complete.
	 */
	public function checkAndRepairTable($tableName) {
		return true;
	}

	public function createField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" ADD \"$fieldName\" $fieldSpec");
	}

	/**
	 * Change the database type of the given field.
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $fieldName The name of the field to change.
	 * @param string $fieldSpec The new field specification
	 */
	public function alterField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" CHANGE \"$fieldName\" \"$fieldName\" $fieldSpec");
	}

	/**
	 * Change the database column name of the given field.
	 *
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $oldName The name of the field to change.
	 * @param string $newName The new name of the field
	 */
	public function renameField($tableName, $oldName, $newName) {
			$this->query("EXEC sp_rename @objname = '$tableName.$oldName', @newname = '$newName', @objtype = 'COLUMN'");
		}

	public function fieldList($table) {
		//This gets us more information than we need, but I've included it all for the moment....
		$fieldRecords = $this->query("SELECT ordinal_position, column_name, data_type, column_default,
			is_nullable, character_maximum_length, numeric_precision, numeric_scale, collation_name
			FROM information_schema.columns WHERE table_name = '$table'
			ORDER BY ordinal_position;");

		// Cache the records from the query - otherwise a lack of multiple active result sets
		// will cause subsequent queries to fail in this method
		$fields = array();
		$output = array();
		foreach($fieldRecords as $record) {
			$fields[] = $record;
		}

		foreach($fields as $field) {
			// Update the data_type field to be a complete column definition string for use by
			// SS_Database::requireField()
			switch($field['data_type']){
				case 'bigint':
				case 'numeric':
				case 'float':
				case 'bit':
					if($field['data_type'] != 'bigint' && $sizeSuffix = $field['numeric_precision']) {
							$field['data_type'] .= "($sizeSuffix)";
					}

					if($field['is_nullable'] == 'YES') {
						$field['data_type'] .= ' null';
					} else {
						$field['data_type'] .= ' not null';
					}
					if($field['column_default']) {
						$default=substr($field['column_default'], 2, -2);
						$field['data_type'] .= " default $default";
					}
					break;

				case 'decimal':
					if($field['numeric_precision']) {
						$sizeSuffix = $field['numeric_precision'] . ',' . $field['numeric_scale'];
							$field['data_type'] .= "($sizeSuffix)";
					}

					if($field['is_nullable'] == 'YES') {
						$field['data_type'] .= ' null';
					} else {
						$field['data_type'] .= ' not null';
					}
					if($field['column_default']) {
						$default=substr($field['column_default'], 2, -2);
						$field['data_type'] .= " default $default";
					}
					break;

				case 'nvarchar':
				case 'varchar':
					//Check to see if there's a constraint attached to this column:
					$clause = $this->getConstraintCheckClause($table, $field['column_name']);
					if($clause) {
						$constraints = $this->enumValuesFromCheckClause($clause);
						$default=substr($field['column_default'], 2, -2);
						$field['data_type'] = $this->enum(array(
							'default' => $default,
							'name' => $field['column_name'],
							'enums' => $constraints,
							'table' => $table
						));
						break;
					}

				default:
					$sizeSuffix = $field['character_maximum_length'];
					if($sizeSuffix == '-1') $sizeSuffix = 'max';
					if($sizeSuffix) {
							$field['data_type'] .= "($sizeSuffix)";
					}

					if($field['is_nullable'] == 'YES') {
						$field['data_type'] .= ' null';
					} else {
						$field['data_type'] .= ' not null';
					}
					if($field['column_default']) {
						$default=substr($field['column_default'], 2, -2);
						$field['data_type'] .= " default '$default'";
					}
			}
			$output[$field['column_name']]=$field;

		}

		return $output;
	}

	/**
	 *
	 * This is a stub function.  Postgres caches the fieldlist results.
	 *
	 * @param string $tableName
	 *
	 * @return boolean
	 */
	function clear_cached_fieldlist($tableName=false){
		return true;
	}

	/**
	 * Create an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see SS_Database::requireIndex() for more details.
	 */
	public function createIndex($tableName, $indexName, $indexSpec) {
		$this->query($this->getIndexSqlDefinition($tableName, $indexName, $indexSpec));
	}

	/**
	 * This takes the index spec which has been provided by a class (ie static $indexes = blah blah)
	 * and turns it into a proper string.
	 * Some indexes may be arrays, such as fulltext and unique indexes, and this allows database-specific
	 * arrays to be created.
	 */
	public function convertIndexSpec($indexSpec){
		if(is_array($indexSpec)){
			//Here we create a db-specific version of whatever index we need to create.
			switch($indexSpec['type']){
				case 'fulltext':
					$indexSpec='fulltext (' . str_replace(' ', '', $indexSpec['value']) . ')';
					break;
				case 'unique':
					$indexSpec='unique (' . $indexSpec['value'] . ')';
					break;
			}
		}

		return $indexSpec;
	}

	/**
	 * Return SQL for dropping and recreating an index
	 */
	protected function getIndexSqlDefinition($tableName, $indexName, $indexSpec) {
		$index = 'ix_' . str_replace('\\', '_', $tableName) . '_' . $indexName;
		$drop = "IF EXISTS (SELECT name FROM sys.indexes WHERE name = '$index') DROP INDEX $index ON \"" . $tableName . "\";";
		
		if(!is_array($indexSpec)) {
			$indexSpec=trim($indexSpec, '()');
			return "$drop CREATE INDEX $index ON \"" . $tableName . "\" (" . $indexSpec . ");";
		} else {
			// create a type-specific index
			if($indexSpec['type'] == 'fulltext') {
				if($this->fullTextEnabled()) {
					// enable fulltext on this table
					$this->createFullTextCatalog();
					$primary_key = $this->getPrimaryKey($tableName);

					$query = '';
					if($primary_key) {
						$query .= "CREATE FULLTEXT INDEX ON \"$tableName\" ({$indexSpec['value']}) KEY INDEX $primary_key WITH CHANGE_TRACKING AUTO;";
					}

					return $query;
				}
			}

			if($indexSpec['type'] == 'unique') {
				if(!is_array($indexSpec['value'])) $columns = preg_split('/ *, */', trim($indexSpec['value']));
				else $columns = $indexSpec['value'];
				$SQL_columnList = implode(', ', $columns);

				return "$drop CREATE UNIQUE INDEX $index ON \"" . $tableName . "\" ($SQL_columnList);";
			}
		}
	}

	function getDbSqlDefinition($tableName, $indexName, $indexSpec){
		return $indexName;
	}

	/**
	 * Alter an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see SS_Database::requireIndex() for more details.
	 */
	public function alterIndex($tableName, $indexName, $indexSpec) {
	    $indexSpec = trim($indexSpec);
	    if($indexSpec[0] != '(') {
	    	list($indexType, $indexFields) = explode(' ',$indexSpec,2);
	    } else {
	    	$indexFields = $indexSpec;
	    }

	    if(!$indexType) {
	    	$indexType = "index";
	    }

    	$this->query("DROP INDEX $indexName ON $tableName;");
		$this->query("ALTER TABLE \"$tableName\" ADD $indexType \"$indexName\" $indexFields");
	}

	/**
	 * Return the list of indexes in a table.
	 * @param string $table The table name.
	 * @return array
	 */
	public function indexList($table) {
		$indexes = DB::query("EXEC sp_helpindex '$table';");
		$prefix = '';
		$indexList = array();

		foreach($indexes as $index) {
			if(strpos($index['index_description'], 'unique') !== false) {
				$prefix='unique ';
			}

			$key = str_replace(', ', ',', $index['index_keys']);
			$indexList[$key]['indexname'] = $index['index_name'];
			$indexList[$key]['spec'] = $prefix . '(' . $key . ')';
		}

		// Now we need to check to see if we have any fulltext indexes attached to this table:
		if($this->fullTextEnabled()) {
			$result = DB::query('EXEC sp_help_fulltext_columns;');
			$columns = '';
			foreach($result as $row) {
				if($row['TABLE_NAME'] == $table) {
					$columns .= $row['FULLTEXT_COLUMN_NAME'] . ',';
				}
			}

			if($columns!=''){
				$columns=trim($columns, ',');
				$indexList['SearchFields']['indexname'] = 'SearchFields';
				$indexList['SearchFields']['spec'] = 'fulltext (' . $columns . ')';
			}
		}

		return $indexList;
	}

	/**
	 * Returns a list of all the tables in the database.
	 * Table names will all be in lowercase.
	 * @return array
	 */
	public function tableList() {
		$tables = array();
		foreach($this->query("EXEC sp_tables @table_owner = 'dbo';") as $record) {
			$tables[strtolower($record['TABLE_NAME'])] = $record['TABLE_NAME'];
		}
		return $tables;
	}

	/**
	 * Empty the given table of all contents.
	 */
	public function clearTable($table) {
		$this->query("TRUNCATE TABLE \"$table\"");
	}

	/**
	 * Return the number of rows affected by the previous operation.
	 * @return int
	 */
	public function affectedRows() {
		if($this->mssql) {
			return mssql_rows_affected($this->dbConn);
		} else {
			return $this->lastAffectedRows;
		}
	}

	/**
	 * Return a boolean type-formatted string
	 * We use 'bit' so that we can do numeric-based comparisons
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function boolean($values) {
		$default = ($values['default']) ? '1' : '0';
		return 'bit not null default ' . $default;
	}

	/**
	 * Return a date type-formatted string.
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values) {
		return 'date null';
	}

	/**
	 * Return a decimal type-formatted string
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function decimal($values) {
		// Avoid empty strings being put in the db
		if($values['precision'] == '') {
			$precision = 1;
		} else {
			$precision = $values['precision'];
		}

		$defaultValue = '0';
		if(isset($values['default']) && is_numeric($values['default'])) {
			$defaultValue = $values['default'];
		}

		return 'decimal(' . $precision . ') not null default ' . $defaultValue;
	}

	/**
	 * Return a enum type-formatted string
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function enum($values) {
		// Enums are a bit different. We'll be creating a varchar(255) with a constraint of all the
		// usual enum options.
		// NOTE: In this one instance, we are including the table name in the values array

		$maxLength = max(array_map('strlen', $values['enums']));

		return "varchar($maxLength) not null default '" . $values['default']
			. "' check(\"" . $values['name'] . "\" in ('" . implode("','", $values['enums'])
			. "'))";
	}

	/**
	 * @todo Make this work like {@link MySQLDatabase::set()}
	 */
	public function set($values) {
		return $this->enum($values);
	}

	/**
	 * Return a float type-formatted string.
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function float($values) {
		return 'float not null default ' . $values['default'];
	}

	/**
	 * Return a int type-formatted string
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values) {
		//We'll be using an 8 digit precision to keep it in line with the serial8 datatype for ID columns
		return 'numeric(8) not null default ' . (int) $values['default'];
	}

	/**
	 * Return a datetime type-formatted string
	 * For MS SQL, we simply return the word 'timestamp', no other parameters are necessary
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function ss_datetime($values) {
		return 'datetime null';
	}

	/**
	 * Return a text type-formatted string
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function text($values) {
		$collation = self::$collation ? " COLLATE " . self::$collation : "";
		return "nvarchar(max)$collation null";
	}

	/**
	 * Return a time type-formatted string.
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function time($values){
		return 'time null';
	}

	/**
	 * Return a varchar type-formatted string
	 *
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function varchar($values) {
		$collation = self::$collation ? " COLLATE " . self::$collation : "";
		return "nvarchar(" . $values['precision'] . ")$collation null";
	}

	/**
	 * Return a 4 digit numeric type.
	 * @return string
	 */
	public function year($values) {
		return 'numeric(4)';
	}

	/**
	 * This returns the column which is the primary key for each table
	 * In Postgres, it is a SERIAL8, which is the equivalent of an auto_increment
	 *
	 * @return string
	 */
	function IdColumn($asDbValue=false, $hasAutoIncPK=true){
		if($asDbValue)
			return 'bigint not null';
		else {
			if($hasAutoIncPK)
				return 'bigint identity(1,1)';
			else return 'bigint not null';
		}
	}

	/**
	 * Returns the SQL command to get all the tables in this database
	 */
	function allTablesSQL(){
		return "SELECT \"name\" FROM \"sys\".\"tables\";";
	}

	/**
	 * Returns true if this table exists
	 * @todo Make a proper implementation
	 */
	function hasTable($tableName) {
		$SQL_tableName = Convert::raw2sql($tableName);
		$value = DB::query("SELECT table_name FROM information_schema.tables WHERE table_name = '$SQL_tableName'")->value();
		return (bool)$value;
	}

	/**
	 * Returns the values of the given enum field
	 * NOTE: Experimental; introduced for db-abstraction and may changed before 2.4 is released.
	 */
	public function enumValuesForField($tableName, $fieldName) {
		$classes = array();

		// Get the enum of all page types from the SiteTree table
		$clause = $this->getConstraintCheckClause($tableName, $fieldName);
		if($clause) {
			$classes = $this->enumValuesFromCheckClause($clause);
		}

		return $classes;
	}

	/**
	 * SQL Server uses CURRENT_TIMESTAMP for the current date/time.
	 */
	function now() {
		return 'CURRENT_TIMESTAMP';
	}

	/**
	 * Returns the database-specific version of the random() function
	 */
	function random(){
		return 'RAND()';
	}

	/**
	 * This is a lookup table for data types.
	 *
	 * For instance, MSSQL uses 'BIGINT', while MySQL uses 'UNSIGNED'
	 * and PostgreSQL uses 'INT'.
	 */
	function dbDataType($type){
		$values = array(
			'unsigned integer'=>'BIGINT'
		);
		if(isset($values[$type])) return $values[$type];
		else return '';
	}

	/**
	 * Convert a SQLQuery object into a SQL statement.
	 *
	 * Needs to be overloaded from {@link Database} because MSSQL has
	 * a very specific way of limiting results from a query.
	 *
	 * @param SQLQuery
	 * @return string SQL text
	 */
	public function sqlQueryToString(SQLQuery $query) {
		// get the limit and offset
		$limit = '';
		$offset = '0';
		$text = '';
		$suffixText = '';
		$nestedQuery = false;

		if(is_array($query->getLimit())) {
			$limitArr = $query->getLimit();
			if(isset($limitArr['limit'])) $limit = $limitArr['limit'];
			if(isset($limitArr['start'])) $offset = $limitArr['start'];
		} else if(preg_match('/^([0-9]+) offset ([0-9]+)$/i', trim($query->getLimit()), $matches)) {
			$limit = $matches[1];
			$offset = $matches[2];
		} else {
			//could be a comma delimited string
			$bits = explode(',', $query->getLimit());
			if(sizeof($bits) > 1) {
				list($offset, $limit) = $bits;
			} else {
				$limit = $bits[0];
			}
		}

		// DELETE queries
		if($query->getDelete()) {
			$text = 'DELETE ';
		} else {
			$distinct = $query->getDistinct() ? 'DISTINCT ' : '';
			// If there's a limit but no offset, just use 'TOP X'
			// rather than the more complex sub-select method
			if ($limit != 0 && $offset == 0) {
				$text = "SELECT $distinct TOP $limit";

			// If there's a limit and an offset, then we need to do a subselect
			} else if($limit && $offset) {
				if($query->getOrderBy()) {
					$orderByClause = $this->sqlOrderByToString($query->getOrderBy());
					$rowNumber = "ROW_NUMBER() OVER ($orderByClause) AS Number";
				} else {
					$selects = $query->getSelect();
					$firstCol = reset($selects);
					$rowNumber = "ROW_NUMBER() OVER (ORDER BY $firstCol) AS Number";
				}
				$text = "SELECT * FROM (SELECT $distinct$rowNumber, ";
				$suffixText .= ") AS Numbered WHERE Number BETWEEN " . ($offset+1) ." AND " . ($offset+$limit)
					. " ORDER BY Number";
				$nestedQuery = true;

			// Otherwise a simple query
			} else {
				$text = "SELECT $distinct";
			}

			// Now add the columns to be selected
			// strip off the SELECT text as it gets done above instead
			$text .= trim(str_replace('SELECT' , '', $this->sqlSelectToString($query->getSelect())));
		}

		if($query->getFrom()) $text .= $this->sqlFromToString($query->getFrom());
		if($query->getWhere()) $text .= $this->sqlWhereToString($query->getWhere(), $query->getConnective());

		// these clauses only make sense in SELECT queries, not DELETE
		if(!$query->getDelete()) {
			if($query->getGroupBy()) $text .= $this->sqlGroupByToString($query->getGroupBy());
			if($query->getHaving()) $text .= $this->sqlHavingToString($query->getHaving());
			if($query->getOrderBy() && !$nestedQuery) $text .= $this->sqlOrderByToString($query->getOrderBy());
		}

		// $suffixText is used by the nested queries to create an offset limit
		if($suffixText) $text .= $suffixText;

		return $text;
	}

	/**
	 * Escapes a value with specific escape characters specific to the MSSQL.
	 * @param string $value String to escape
	 * @return string Escaped string
	 */
	function addslashes($value){
    	$value=str_replace("'","''",$value);
    	$value=str_replace("\0","[NULL]",$value);

    	return $value;
	}

	/**
	 * This changes the index name depending on database requirements.
	 * MSSQL requires underscores to be replaced with commas.
	 */
	function modifyIndex($index) {
		return str_replace('_', ',', $index);
	}

	/**
	 * The core search engine configuration.
	 * Picks up the fulltext-indexed tables from the database and executes search on all of them.
	 * Results are obtained as ID-ClassName pairs which is later used to reconstruct the DataObjectSet.
	 *
	 * @param array classesToSearch computes all descendants and includes them. Check is done via WHERE clause.
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "Relevance DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		if(isset($objects)) $results = new ArrayList($objects);
		else $results = new ArrayList();

		if (!$this->fullTextEnabled()) return $results;
		if (!in_array(substr($sortBy, 0, 9), array('"Relevanc', 'Relevance'))) user_error("Non-relevance sort not supported.", E_USER_ERROR);

		$allClassesToSearch = array();
		foreach ($classesToSearch as $class) {
			$allClassesToSearch = array_merge($allClassesToSearch, ClassInfo::dataClassesFor($class));
		}
		$allClassesToSearch = array_unique($allClassesToSearch);

		//Get a list of all the tables and columns we'll be searching on:
		$fulltextColumns = DB::query('EXEC sp_help_fulltext_columns');
		$queries = array();

		// Sort the columns back into tables.
		$tables = array();
		foreach($fulltextColumns as $column) {
			// Skip extension tables.
			if(substr($column['TABLE_NAME'], -5)=='_Live' || substr($column['TABLE_NAME'], -9)=='_versions') continue;

			// Add the column to table.
			$table = &$tables[$column['TABLE_NAME']];
			if (!$table) $table = array($column['FULLTEXT_COLUMN_NAME']);
			else array_push($table, $column['FULLTEXT_COLUMN_NAME']);
		}

		// Create one query per each table, $columns not used. We want just the ID and the ClassName of the object from this query.
		foreach($tables as $tableName=>$columns){
			$baseClass = ClassInfo::baseDataClass($tableName);

			$join = $this->fullTextSearchMSSQL($tableName, $keywords);
			if (!$join) return $results; // avoid "Null or empty full-text predicate"

			// Check if we need to add ShowInSearch
			$where = null;
			if(strpos($tableName, 'SiteTree') === 0) {
				$where = array("\"$tableName\".\"ShowInSearch\"!=0");
			} elseif(strpos($tableName, 'File') === 0) {
				// File.ShowInSearch was added later, keep the database driver backwards compatible
				// by checking for its existence first
				$fields = $this->fieldList($tableName);
				if(array_key_exists('ShowInSearch', $fields)) {
					$where = array("\"$tableName\".\"ShowInSearch\"!=0");
				}
			}

			$queries[$tableName] = DataList::create($tableName)->where($where, '')->dataQuery()->query();
			$queries[$tableName]->setOrderBy(array());
			
			// Join with CONTAINSTABLE, a full text searcher that includes relevance factor
			$queries[$tableName]->setFrom(array("\"$tableName\" INNER JOIN $join AS \"ft\" ON \"$tableName\".\"ID\"=\"ft\".\"KEY\""));
			// Join with the base class if needed, as we want to test agains the ClassName
			if ($tableName != $baseClass) {
				$queries[$tableName]->setFrom("INNER JOIN \"$baseClass\" ON  \"$baseClass\".\"ID\"=\"$tableName\".\"ID\"");
			}

			$queries[$tableName]->setSelect(array("\"$tableName\".\"ID\""));
			$queries[$tableName]->selectField("'$tableName'", 'Source');
			$queries[$tableName]->selectField('Rank', 'Relevance');
			if ($extraFilter) {
				$queries[$tableName]->addWhere($extraFilter);
			}
			if (count($allClassesToSearch)) {
				$queries[$tableName]->addWhere("\"$baseClass\".\"ClassName\" IN ('".implode($allClassesToSearch, "', '")."')");
			}
			// Reset the parameters that would get in the way

		}

		// Generate SQL
		$querySQLs = array();
		foreach($queries as $query) {
			$querySQLs[] = $query->sql();
		}

		// Unite the SQL
		$fullQuery = implode(" UNION ", $querySQLs) . " ORDER BY $sortBy";

		// Perform the search
		$result = DB::query($fullQuery);

		// Regenerate DataObjectSet - watch out, numRecords doesn't work on sqlsrv driver on Windows.
		$current = -1;
		$objects = array();
		foreach ($result as $row) {
			$current++;

			// Select a subset for paging
			if ($current >= $start && $current < $start + $pageLength) {
				$objects[] = DataObject::get_by_id($row['Source'], $row['ID']);
			}
		}

		if(isset($objects)) $results = new ArrayList($objects);
		else $results = new ArrayList();
		$list = new PaginatedList($results);
		$list->setPageStart($start);
		$list->setPageLength($pageLength);
		$list->setTotalItems($current+1);
		return $list;
	}

	/**
	 * Allow auto-increment primary key editing on the given table.
	 * Some databases need to enable this specially.
	 * @param $table The name of the table to have PK editing allowed on
	 * @param $allow True to start, false to finish
	 */
	function allowPrimaryKeyEditing($table, $allow = true) {
		$this->query("SET IDENTITY_INSERT \"$table\" " . ($allow ? "ON" : "OFF"));
	}

	/**
	 * Check if a fulltext index exists on a particular table name.
	 * @return boolean TRUE index exists | FALSE index does not exist | NULL no support
	 */
	function fulltextIndexExists($tableName) {
		// Special case for no full text index support
		if(!$this->fullTextEnabled()) return null;

		return (bool) $this->query("
			SELECT 1 FROM sys.fulltext_indexes i
			JOIN sys.objects o ON i.object_id = o.object_id
			WHERE o.name = '$tableName'
			")->value();
	}

	/**
	 * Returns a SQL fragment for querying a fulltext search index
	 *
	 * @param $tableName specific - table name
	 * @param $keywords string The search query
	 * @param $fields array The list of field names to search on, or null to include all
	 *
	 * @returns null if keyword set is empty or the string with JOIN clause to be added to SQL query
	 */
	function fullTextSearchMSSQL($tableName, $keywords, $fields = null) {
		// Make sure we are getting an array of fields
		if (isset($fields) && !is_array($fields)) $fields = array($fields);

		// Strip unfriendly characters, SQLServer "CONTAINS" predicate will crash on & and | and ignore others anyway.
		if (function_exists('mb_ereg_replace')) {
			$keywords = mb_ereg_replace('[^\w\s]', '', trim($keywords));
		}
		else {
			$keywords = Convert::raw2sql(str_replace(array('&','|','!','"','\''), '', trim($keywords)));
		}

		// Remove stopwords, concat with ANDs
		$keywords = explode(' ', $keywords);
		$keywords = self::removeStopwords($keywords);
		$keywords = implode(' AND ', $keywords);

		if (!$keywords || trim($keywords)=='') return null;

		if ($fields) $fieldNames = '"' . implode('", "', $fields) . '"';
		else $fieldNames = "*";

		return "CONTAINSTABLE(\"$tableName\", ($fieldNames), '$keywords')";
	}

	/**
	 * Remove stopwords that would kill a MSSQL full-text query
	 *
	 * @param array $keywords
	 *
	 * @return array $keywords with stopwords removed
	 */
	static public function removeStopwords($keywords) {
		$goodKeywords = array();
		foreach($keywords as $keyword) {
			if (in_array($keyword, self::$noiseWords)) continue;
			$goodKeywords[] = trim($keyword);
		}
		return $goodKeywords;
	}

	/**
	 * Does this database support transactions?
	 */
	public function supportsTransactions(){
		return $this->supportsTransactions;
	}

	/**
	 * This is a quick lookup to discover if the database supports particular extensions
	 * Currently, MSSQL supports no extensions
	 */
	public function supportsExtensions($extensions=Array('partitions', 'tablespaces', 'clustering')){
		if(isset($extensions['partitions']))
			return false;
		elseif(isset($extensions['tablespaces']))
			return false;
		elseif(isset($extensions['clustering']))
			return false;
		else
			return false;
	}
	
	/**
	 * @deprecated Use transactionStart() (method required for 2.4.x)
	 */
	public function startTransaction($transaction_mode=false, $session_characteristics=false){
		$this->transactionStart($transaction_mode, $session_characteristics);
	}

	/**
	 * Start transaction. READ ONLY not supported.
	 */
	public function transactionStart($transaction_mode=false, $session_characteristics=false){
		if($this->mssql) {
			DB::query('BEGIN TRANSACTION');
		} else {
			$result = sqlsrv_begin_transaction($this->dbConn);
			if (!$result) $this->databaseError("Couldn't start the transaction.", E_USER_ERROR);
		}
	}

	/**
	 * Create a savepoint that you can jump back to if you encounter problems
	 */
	public function transactionSavepoint($savepoint){
		DB::query("SAVE TRANSACTION \"$savepoint\"");
	}

	/**
	 * Rollback or revert to a savepoint if your queries encounter problems
	 * If you encounter a problem at any point during a transaction, you may
	 * need to rollback that particular query, or return to a savepoint
	 */
	public function transactionRollback($savepoint=false){
		if($savepoint) {
			DB::query("ROLLBACK TRANSACTION \"$savepoint\"");
		} else {
			if($this->mssql) {
				DB::query('ROLLBACK TRANSACTION');
			} else {
				$result = sqlsrv_rollback($this->dbConn);
				if (!$result) $this->databaseError("Couldn't rollback the transaction.", E_USER_ERROR);
			}
		}
	}
	
	/**
	 * @deprecated Use transactionEnd() (method required for 2.4.x)
	 */
	public function endTransaction(){
		$this->transactionEnd();
	}

	/**
	 * Commit everything inside this transaction so far
	 */
	public function transactionEnd(){
		if($this->mssql) {
			DB::query('COMMIT TRANSACTION');
		} else {
			$result = sqlsrv_commit($this->dbConn);
			if (!$result) $this->databaseError("Couldn't commit the transaction.", E_USER_ERROR);
		}
	}

	/**
	 * Overload the Database::prepStringForDB() method and include "N" prefix so unicode
	 * strings are saved to the database correctly.
	 *
	 * @param string $string String to be encoded
	 * @return string Processed string ready for DB
	 */
	public function prepStringForDB($string) {
		return "N'" . Convert::raw2sql($string) . "'";
	}

	/**
	 * Function to return an SQL datetime expression for MSSQL
	 * used for querying a datetime in a certain format
	 * @param string $date to be formated, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $format to be used, supported specifiers:
	 * %Y = Year (four digits)
	 * %m = Month (01..12)
	 * %d = Day (01..31)
	 * %H = Hour (00..23)
	 * %i = Minutes (00..59)
	 * %s = Seconds (00..59)
	 * %U = unix timestamp, can only be used on it's own
	 * @return string SQL datetime expression to query for a formatted datetime
	 */
	function formattedDatetimeClause($date, $format) {
		preg_match_all('/%(.)/', $format, $matches);
		foreach($matches[1] as $match) if(array_search($match, array('Y','m','d','H','i','s','U')) === false) user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);

		if(preg_match('/^now$/i', $date)) {
			$date = "CURRENT_TIMESTAMP";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "'$date.000'";
		}

		if($format == '%U') {
			return "DATEDIFF(s, '1970-01-01 00:00:00', DATEADD(hour, DATEDIFF(hour, GETDATE(), GETUTCDATE()), $date))";
		}

		$trans = array(
			'Y' => 'yy',
			'm' => 'mm',
			'd' => 'dd',
			'H' => 'hh',
			'i' => 'mi',
			's' => 'ss',
		);

		$strings = array();
		$buffer = $format;
		while(strlen($buffer)) {
			if(substr($buffer,0,1) == '%') {
				$f = substr($buffer,1,1);
				$flen = $f == 'Y' ? 4 : 2;
				$strings[] = "RIGHT('0' + CAST(DATEPART({$trans[$f]},$date) AS VARCHAR), $flen)";
				$buffer = substr($buffer, 2);
			} else {
				$pos = strpos($buffer, '%');
				if($pos === false) {
					$strings[] = $buffer;
					$buffer = '';
				} else {
					$strings[] = "'".substr($buffer, 0, $pos)."'";
					$buffer = substr($buffer, $pos);
				}
			}
		}

		return '(' . implode(' + ', $strings) . ')';

	}

	/**
	 * Function to return an SQL datetime expression for MSSQL.
	 * used for querying a datetime addition
	 * @param string $date, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $interval to be added, use the format [sign][integer] [qualifier], e.g. -1 Day, +15 minutes, +1 YEAR
	 * supported qualifiers:
	 * - years
	 * - months
	 * - days
	 * - hours
	 * - minutes
	 * - seconds
	 * This includes the singular forms as well
	 * @return string SQL datetime expression to query for a datetime (YYYY-MM-DD hh:mm:ss) which is the result of the addition
	 */
	function datetimeIntervalClause($date, $interval) {
		$trans = array(
			'year' => 'yy',
			'month' => 'mm',
			'day' => 'dd',
			'hour' => 'hh',
			'minute' => 'mi',
			'second' => 'ss',
		);

		$singularinterval = preg_replace('/(year|month|day|hour|minute|second)s/i', '$1', $interval);

		if(
			!($params = preg_match('/([-+]\d+) (\w+)/i', $singularinterval, $matches)) ||
			!isset($trans[strtolower($matches[2])])
		)  user_error('datetimeIntervalClause(): invalid interval ' . $interval, E_USER_WARNING);

		if(preg_match('/^now$/i', $date)) {
			$date = "CURRENT_TIMESTAMP";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "'$date'";
		}

		return "CONVERT(VARCHAR, DATEADD(" . $trans[strtolower($matches[2])] . ", " . (int)$matches[1] . ", $date), 120)";
	}

	/**
	 * Function to return an SQL datetime expression for MSSQL.
	 * used for querying a datetime substraction
	 * @param string $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $date2 to be substracted of $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @return string SQL datetime expression to query for the interval between $date1 and $date2 in seconds which is the result of the substraction
	 */
	function datetimeDifferenceClause($date1, $date2) {

		if(preg_match('/^now$/i', $date1)) {
			$date1 = "CURRENT_TIMESTAMP";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
			$date1 = "'$date1'";
		}

		if(preg_match('/^now$/i', $date2)) {
			$date2 = "CURRENT_TIMESTAMP";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
			$date2 = "'$date2'";
		}

		return "DATEDIFF(s, $date2, $date1)";
	}
}

/**
 * A result-set from a MSSQL database.
 * @package sapphire
 * @subpackage model
 */
class MSSQLQuery extends SS_Query {

	/**
	 * The MSSQLDatabase object that created this result set.
	 * @var MSSQLDatabase
	 */
	private $database;

	/**
	 * The internal MSSQL handle that points to the result set.
	 * @var resource
	 */
	private $handle;

	/**
	 * If true, use the mssql_... functions.
	 * If false use the sqlsrv_... functions
	 */
	private $mssql = null;

	/**
	 * A list of field meta-data, such as column name and data type.
	 * @var array
	 */
	private $fields = array();

	/**
	 * Hook the result-set given into a Query class, suitable for use by sapphire.
	 * @param database The database object that created this query.
	 * @param handle the internal mssql handle that is points to the resultset.
	 */
	public function __construct(MSSQLDatabase $database, $handle, $mssql) {
		$this->database = $database;
		$this->handle = $handle;
		$this->mssql = $mssql;

		// build a list of field meta-data for this query we'll use in nextRecord()
		// doing it here once saves us from calling mssql_fetch_field() in nextRecord()
		// potentially hundreds of times, which is unnecessary.
		if($this->mssql && is_resource($this->handle)) {
			for($i = 0; $i < mssql_num_fields($handle); $i++) {
				$this->fields[$i] = mssql_fetch_field($handle, $i);
			}
		}
	}

	public function __destruct() {
		if(is_resource($this->handle)) {
			if($this->mssql) {
				mssql_free_result($this->handle);
			} else {
				sqlsrv_free_stmt($this->handle);
			}
		}
	}

	public function seek($row) {
		if(!is_resource($this->handle)) return false;

		if($this->mssql) {
			return mssql_data_seek($this->handle, $row);
		} else {
			user_error('MSSQLQuery::seek() not supported in sqlsrv', E_USER_WARNING);
		}
	}

	public function numRecords() {
		if(!is_resource($this->handle)) return false;

		if($this->mssql) {
			return mssql_num_rows($this->handle);
		} else {
			// WARNING: This will only work if the cursor type is scrollable!
			if(function_exists('sqlsrv_num_rows')) {
				return sqlsrv_num_rows($this->handle);
			} else {
				user_error('MSSQLQuery::numRecords() not supported in this version of sqlsrv', E_USER_WARNING);
			}
		}
	}

	public function nextRecord() {
		if(!is_resource($this->handle)) return false;

		if($this->mssql) {
			if($row = mssql_fetch_row($this->handle)) {
				foreach($row as $i => $value) {
					$field = $this->fields[$i];

					// fix datetime formatting from format "Jan  1 2012 12:00:00:000AM" to "2012-01-01 12:00:00"
					// strtotime doesn't understand this format, so we need to do some modification of the value first
					if($field->type == 'datetime' && $value) {
						$value = date('Y-m-d H:i:s', strtotime(preg_replace('/:[0-9][0-9][0-9]([ap]m)$/i', ' \\1', $value)));
					}

					if(isset($value) || !isset($data[$field->name])) {
						$data[$field->name] = $value;
					}
				}
				return $data;
			}
		} else {
			if($data = sqlsrv_fetch_array($this->handle, SQLSRV_FETCH_ASSOC)) {

				// special case for sqlsrv - date values are DateTime coming out of the sqlsrv drivers,
				// so we convert to the usual Y-m-d H:i:s value!
				foreach($data as $name => $value) {
					if($value instanceof DateTime) $data[$name] = $value->format('Y-m-d H:i:s');
				}
				return $data;
			} else {
				// Free the handle if there are no more results - sqlsrv crashes if there are too many handles
				sqlsrv_free_stmt($this->handle);
				$this->handle = null;
			}
		}

		return false;
	}

}
