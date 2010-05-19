<?php
/**
 * Microsoft SQL Server 2008 connector class.
 * 
 * Connecting using Windows:
 * If you've got your website running on Windows, it's highly recommended you
 * use "sqlsrv", a SQL Server driver for PHP by Microsoft.
 * @see http://www.microsoft.com/downloads/details.aspx?displaylang=en&FamilyID=ccdf728b-1ea0-48a8-a84a-5052214caad9
 * 
 * Connecting using a UNIX platform:
 * On other platforms such as Mac OS X and Linux, you'll have to use FreeTDS.
 * PHP also needs to be built with mssql enabled. This is easy if you use XAMPP,
 * as it's already included.
 * If using MacPorts, you can use "port install php5-mssql" if you've already got
 * the php5 port installed.
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
	public static $noiseWords = array("about", "1", "after", "2", "all", "also", "3", "an", "4", "and", "5", "another", "6", "any", "7", "are", "8", "as", "9", "at", "0", "be", "$", "because", "been", "before", "being", "between", "both", "but", "by", "came", "can", "come", "could", "did", "do", "does", "each", "else", "for", "from", "get", "got", "has", "had", "he", "have", "her", "here", "him", "himself", "his", "how", "if", "in", "into", "is", "it", "its", "just", "like", "make", "many", "me", "might", "more", "most", "much", "must", "my", "never", "no", "now", "of", "on", "only", "or", "other", "our", "out", "over", "re", "said", "same", "see", "should", "since", "so", "some", "still", "such", "take", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "up", "use", "very", "want", "was", "way", "we", "well", "were", "what", "when", "where", "which", "while", "who", "will", "with", "would", "you", "your", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
	
	protected $supportsTransactions = false;
	
	/**
	 * Cached flag to determine if full-text is enabled. This is set by
	 * {@link MSSQLDatabase::fullTextEnabled()}
	 * 
	 * @var boolean
	 */
	protected $fullTextEnabled = null;
	
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
			$this->dbConn = mssql_connect($parameters['server'], $parameters['username'], $parameters['password'], true);
		} else {
			// Disable default warnings as errors behaviour for sqlsrv to keep it in line with mssql functions
			if(ini_get('sqlsrv.WarningsReturnAsErrors')) {
				ini_set('sqlsrv.WarningsReturnAsErrors', 'Off');
			}

			// Windows authentication doesn't require a username and password
			if(defined('MSSQL_USE_WINDOWS_AUTHENTICATION') && MSSQL_USE_WINDOWS_AUTHENTICATION == true) {
				$connectionInfo = array();
			} else {
				$connectionInfo = array(
					'UID' => $parameters['username'],
					'PWD' => $parameters['password'],
				);
			}
			$this->dbConn = sqlsrv_connect($parameters['server'], $connectionInfo);
		}

		if(!$this->dbConn) {
			$this->databaseError("Couldn't connect to MS SQL database");

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
			foreach(sqlsrv_errors() as $error) {
				$errorMessages[] = $error['message'];
			}
			$message .= ": \n" . implode("; ",$errorMessages);
		}
		
		return parent::databaseError($message, $errorLevel);
	}
	
	/**
	 * This will set up the full text search capabilities.
	 *
	 * TODO: make this a _config.php setting
	 * TODO: VERY IMPORTANT: move this so it only gets called upon a dev/build action
	 */
	function createFullTextCatalog() {
		if($this->fullTextEnabled()) {
			$result = $this->query("SELECT name FROM sys.fulltext_catalogs WHERE name = 'ftCatalog';")->value();
			if(!$result) $this->query("CREATE FULLTEXT CATALOG ftCatalog AS DEFAULT;");
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
	
	/**
	 * Get the version of MSSQL.
	 * NOTE: not yet implemented for MSSQL, we just return 2008; the minimum supported version
	 * @return float
	 */
	public function getVersion() {
		user_error("getVersion not implemented", E_USER_WARNING);
		return 2008;
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
				$error = sqlsrv_errors();
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
		//return $this->query("SELECT @@IDENTITY FROM \"$table\"")->value();
		return $this->query("SELECT IDENT_CURRENT('$table')")->value();
	}
	
	/*
	 * This is a handy helper function which will return the primary key for any paricular table
	 * In MSSQL, the primary key is often an internal identifier, NOT the standard name (ie, 'ID'),
	 * so we need to do a lookup for it.
	 */
	function getPrimaryKey($tableName){
		$indexes=DB::query("EXEC sp_helpindex '$tableName';");
		$primary_key='';
		foreach($indexes as $this_index){
			if($this_index['index_keys']=='ID'){
				$primary_key=$this_index['index_name'];
				break;
			}
		}
		
		return $primary_key;
	}
	
	/**
	 * OBSOLETE: Get the ID for the next new record for the table.
	 * @param string $table The name of the table
	 * @return int
	 */
	public function getNextID($table) {
		user_error('getNextID is OBSOLETE (and will no longer work properly)', E_USER_WARNING);
		$result = $this->query("SELECT MAX(ID)+1 FROM \"$table\"")->value();
		return $result ? $result : 1;
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
	 * Check if the given database exists.
	 * @param string $name Name of database to check exists
	 * @return boolean
	 */
	public function databaseExists($name) {
		$listDBs = $this->query('SELECT NAME FROM sys.sysdatabases');
		if($listDBs) {
			foreach($listDBs as $listedDB) {
				if($listedDB['NAME'] == $name) return true;
			}
		}
		return false;
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
		$fieldSchemas = $indexSchemas = "";
		$alterList = array();
		$indexList = $this->indexList($tableName);
		
		if($newFields) foreach($newFields as $k => $v) $alterList[] .= "ALTER TABLE \"$tableName\" ADD \"$k\" $v";
		if($alteredFields) foreach($alteredFields as $k => $v) {
			$val = $this->alterTableAlterColumn($tableName, $k, $v, $indexList);
			if($val != '') $alterList[] .= $val;
		}
		
		if($alteredIndexes) foreach($alteredIndexes as $k => $v) $alterList[] .= $this->getIndexSqlDefinition($tableName, $k, $v);
		if($newIndexes) foreach($newIndexes as $k =>$v) $alterList[] .= $this->getIndexSqlDefinition($tableName, $k, $v);

		if($alterList) {
			foreach($alterList as $alteration) {
				if($alteration != '') {
					$this->query($alteration);
				}
			}
		}
	}
	
	/**
	 * This is a private MSSQL-only function which returns
	 * specific details about a column's constraints (if any)
	 * @param string $tableName Name of table the column exists in
	 * @param string $columnName Name of column to check for
	 */
	protected function ColumnConstraints($tableName, $columnName) {
		$constraint = $this->query("SELECT CC.CONSTRAINT_NAME, CAST(CHECK_CLAUSE AS TEXT) AS CHECK_CLAUSE, COLUMN_NAME FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS CC INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME=CC.CONSTRAINT_NAME WHERE TABLE_NAME='$tableName' AND COLUMN_NAME='" . $columnName . "';")->first();
		return $constraint;
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
	 * Get the actual enum fields from the constraint value:
	 */
	protected function EnumValuesFromConstraint($constraint){
		$segments=preg_split('/ +OR *\[/i', $constraint);
		$constraints=Array();
		foreach($segments as $this_segment){
			$bits=preg_split('/ *= */', $this_segment);
			
			for($i=1; $i<sizeof($bits); $i+=2)
				array_unshift($constraints, substr(rtrim($bits[$i], ')'), 1, -1));
			
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
		
		// fulltext indexes need to be dropped if alterting a table
		if($this->fulltextIndexExists($tableName) === true) {
			$alterCol .= "\nDROP FULLTEXT INDEX ON \"$tableName\";";
		}

		$prefix="ALTER TABLE \"" . $tableName . "\" ";

		// Remove the old default prior to adjusting the column.
		if($defaultConstraintName = $this->defaultConstraintName($tableName, $colName)) {
			$alterCol .= ";\n$prefix DROP CONSTRAINT \"$defaultConstraintName\"";
		}
		
		if(isset($matches[1])) {
			//We will prevent any changes being made to the ID column.  Primary key indexes will have a fit if we do anything here.
			if($colName!='ID'){
				$alterCol .= ";\n$prefix ALTER COLUMN \"$colName\" $matches[1]";
			
				// SET null / not null
				if(!empty($matches[2])) $alterCol .= ";\n$prefix ALTER COLUMN \"$colName\" $matches[1] $matches[2]";
	
				// Add a default back
				if(!empty($matches[3])) $alterCol .= ";\n$prefix ADD $matches[3] FOR \"$colName\"";
	
				// SET check constraint (The constraint HAS to be dropped)
				if(!empty($matches[4])) {
					$constraint=$this->ColumnConstraints($tableName, $colName);
					if($constraint)
						$alterCol .= ";\n$prefix DROP CONSTRAINT {$constraint['CONSTRAINT_NAME']}";
						
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
	
	/**
	 * Helper function used by checkAndRepairTable.
	 * @param string $sql Query to run.
	 * @return boolean Returns if the query returns a successful result.
	 */
	protected function runTableCheckCommand($sql) {
		$testResults = $this->query($sql);
		foreach($testResults as $testRecord) {
			if(strtolower($testRecord['Msg_text']) != 'ok') {
				return false;
			}
		}
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
		$fieldList = $this->fieldList($tableName);
		if(array_key_exists($oldName, $fieldList)) {
			$this->query("EXEC sp_rename @objname = '$tableName.$oldName', @newname = '$newName', @objtype = 'COLUMN'");
		}
	}
	
	public function fieldList($table) {
		//This gets us more information than we need, but I've included it all for the moment....
		$fieldRecords = $this->query("SELECT ordinal_position, column_name, data_type, column_default, 
			is_nullable, character_maximum_length, numeric_precision, numeric_scale
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
				
				case 'varchar':
					//Check to see if there's a constraint attached to this column:
					$constraint=$this->ColumnConstraints($table, $field['column_name']);
					if($constraint){
						$constraints=$this->EnumValuesFromConstraint($constraint['CHECK_CLAUSE']);
						$default=substr($field['column_default'], 2, -2);
						$field['data_type']=$this->enum(Array('default'=>$default, 'name'=>$field['column_name'], 'enums'=>$constraints, 'table'=>$table));
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
	
	protected function getIndexSqlDefinition($tableName, $indexName, $indexSpec) {
		if(!is_array($indexSpec)) {
			$indexSpec=trim($indexSpec, '()');
			$bits=explode(',', $indexSpec);
			$indexes="\"" . implode("\",\"", $bits) . "\"";
			$index = 'ix_' . $tableName . '_' . $indexName;
			
			$drop = "IF EXISTS (SELECT name FROM sys.indexes WHERE name = '$index') DROP INDEX $index ON \"" . $tableName . "\";";
			return "$drop CREATE INDEX $index ON \"" . $tableName . "\" (" . $indexes . ");";
		} else {
			//create a type-specific index
			if($indexSpec['type'] == 'fulltext') {
				if($this->fullTextEnabled()) {
					//Enable full text search.
					$this->createFullTextCatalog();
					
					$primary_key=$this->getPrimaryKey($tableName);
					
					$drop = '';
					if($this->fulltextIndexExists($tableName) === true) {
						$drop = "DROP FULLTEXT INDEX ON \"$tableName\";";
					}
					
					return $drop . "CREATE FULLTEXT INDEX ON \"$tableName\" ({$indexSpec['value']}) KEY INDEX $primary_key WITH CHANGE_TRACKING AUTO;";
				}
			}
			
			if($indexSpec['type'] == 'unique') {
				return 'CREATE UNIQUE INDEX ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (\"" . $indexSpec['value'] . "\");";
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
		$indexes=DB::query("EXEC sp_helpindex '$table';");
		$prefix = '';
		$indexList = array();
		
		foreach($indexes as $index) {
			
			//Check for uniques:
			if(strpos($index['index_description'], 'unique')!==false)
				$prefix='unique ';
			
			$key=str_replace(', ', ',', $index['index_keys']);
			$indexList[$key]['indexname']=$index['index_name'];
			$indexList[$key]['spec']=$prefix . '(' . $key . ')';
  		  			
  		}
  		
  		//Now we need to check to see if we have any fulltext indexes attached to this table:
		if($this->fullTextEnabled()) {
	  		$result=DB::query('EXEC sp_help_fulltext_columns;');
	  		$columns='';
	  		foreach($result as $row){
  			
	  			if($row['TABLE_NAME']==$table)
	  				$columns.=$row['FULLTEXT_COLUMN_NAME'] . ',';	
  			
	  		}
  		
	  		if($columns!=''){
	  			$columns=trim($columns, ',');
	  			$indexList['SearchFields']['indexname']='SearchFields';
	  			$indexList['SearchFields']['spec']='fulltext (' . $columns . ')';
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
			$table = strtolower($record['TABLE_NAME']);
			$tables[$table] = $table;
		}
		return $tables;
	}
	
	/**
	 * Empty the given table of call contentTR
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
		//Annoyingly, we need to do a good ol' fashioned switch here:
		($values['default']) ? $default='1' : $default='0';
		return 'bit not null default ' . $default;
	}
	
	/**
	 * Return a date type-formatted string.
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values) {
		return 'datetime null';
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
		return 'nvarchar(max) null';
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
		return 'nvarchar(' . $values['precision'] . ') null';
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
		return true;
	}
	
	/**
	 * Returns the values of the given enum field
	 * NOTE: Experimental; introduced for db-abstraction and may changed before 2.4 is released.
	 */
	public function enumValuesForField($tableName, $fieldName) {
		// Get the enum of all page types from the SiteTree table
		
		$constraints=$this->ColumnConstraints($tableName, $fieldName);
		$classes=Array();
		if($constraints){
			$constraints=$this->EnumValuesFromConstraint($constraints['CHECK_CLAUSE']);
			$classes=$constraints;
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
	 */
	public function sqlQueryToString(SQLQuery $sqlQuery) {
		if (!$sqlQuery->from) return '';
		
		if($sqlQuery->orderby && strtoupper(trim($sqlQuery->orderby)) == 'RAND()') $sqlQuery->orderby = "NEWID()";
		
		//Get the limit and offset
		$limit='';
		$offset='0';
		if(is_array($sqlQuery->limit)){
			$limit=$sqlQuery->limit['limit'];
			if(isset($sqlQuery->limit['start']))
				$offset=$sqlQuery->limit['start'];
			
		} else if(preg_match('/^([0-9]+) offset ([0-9]+)$/i', trim($sqlQuery->limit), $matches)) {
			$limit = $matches[1];
			$offset = $matches[2];
		} else {
			//could be a comma delimited string
			$bits=explode(',', $sqlQuery->limit);
			if(sizeof($bits) > 1) {
				list($offset, $limit) = $bits;
			} else {
				$limit = $bits[0];
			}
		}
		
		$text = '';
		$suffixText = '';
		$nestedQuery = false;

		// DELETE queries
		if($sqlQuery->delete) {
			$text = 'DELETE ';
			
		// SELECT queries
		} else {
			$distinct = $sqlQuery->distinct ? "DISTINCT " : "";
		
			// If there's a limit but no offset, just use 'TOP X'
			// rather than the more complex sub-select method
			if ($limit != 0 && $offset == 0) {
				$text = "SELECT $distinct TOP $limit";
			
			// If there's a limit and an offset, then we need to do a subselect
			} else if($limit && $offset) {
				if($sqlQuery->orderby) {
					$rowNumber = "ROW_NUMBER() OVER (ORDER BY $sqlQuery->orderby) AS Number";
				} else {
					$firstCol = reset($sqlQuery->select);
					$rowNumber = "ROW_NUMBER() OVER (ORDER BY $firstCol) AS Number";
				}
				$text = "SELECT * FROM ( SELECT $distinct$rowNumber, ";
				$suffixText .= ") AS Numbered WHERE Number BETWEEN " . ($offset+1) ." AND " . ($offset+$limit)
					. " ORDER BY Number";
				$nestedQuery = true;

			// Otherwise a simple query
			} else {
				$text = "SELECT $distinct";
			}
			
			// Now add the columns to be selected
			$text .= implode(", ", $sqlQuery->select);
		}

		$text .= " FROM " . implode(" ", $sqlQuery->from);
		if($sqlQuery->where) $text .= " WHERE (" . $sqlQuery->getFilter(). ")";
		if($sqlQuery->groupby) $text .= " GROUP BY " . implode(", ", $sqlQuery->groupby);
		if($sqlQuery->having) $text .= " HAVING ( " . implode(" ) AND ( ", $sqlQuery->having) . " )";
		if(!$nestedQuery && $sqlQuery->orderby) $text .= " ORDER BY " . $sqlQuery->orderby;
		
		// $suffixText is used by the nested queries to create an offset limit
		if($suffixText) $text .= $suffixText;
		
		return $text;
	}
	
	/*
	 * This will return text which has been escaped in a database-friendly manner
	 * Using PHP's addslashes method won't work in MSSQL
	 */
	function addslashes($value){
		$value=stripslashes($value);
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
	 * @todo There is no result relevancy or ordering as it currently stands.
	 * 
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "Relevance DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		$results = new DataObjectSet();
		if(!$this->fullTextEnabled()) {
			return $results;
		}
		
		$keywords = Convert::raw2sql(trim($keywords));
		$htmlEntityKeywords = htmlentities($keywords);
		
		$keywordList = explode(' ', $keywords);
		if($keywordList) {
			foreach($keywordList as $index => $keyword) {
				$keywordList[$index] = "\"{$keyword}\"";
			}
			$keywords = implode(' AND ', $keywordList);
		}
		
		$htmlEntityKeywordList = explode(' ', $htmlEntityKeywords);
		if($htmlEntityKeywordList) {
			foreach($htmlEntityKeywordList as $index => $keyword) {
				$htmlEntityKeywordList[$index] = "\"{$keyword}\"";
			}
			$htmlEntityKeywords = implode(' AND ', $htmlEntityKeywordList);
		}
		
		//Get a list of all the tables and columns we'll be searching on:
		$result = DB::query('EXEC sp_help_fulltext_columns');
		$tables = array();
		
		foreach($result as $row){
			if(substr($row['TABLE_NAME'], -5)!='_Live' && substr($row['TABLE_NAME'], -9)!='_versions') {
				$thisSql = "SELECT ID, '{$row['TABLE_NAME']}' AS Source FROM \"{$row['TABLE_NAME']}\" WHERE (".
						"(CONTAINS(\"{$row['FULLTEXT_COLUMN_NAME']}\", '$keywords') OR CONTAINS(\"{$row['FULLTEXT_COLUMN_NAME']}\", '$htmlEntityKeywords'))";
				if(strpos($row['TABLE_NAME'], 'SiteTree') === 0) {
					$thisSql .= " AND ShowInSearch != 0)";//" OR (Title LIKE '%$keywords%' OR Title LIKE '%$htmlEntityKeywords%')";
				} else {
					$thisSql .= ')';
				}
				
				$tables[] = $thisSql;
			}
		}

		$query = implode(' UNION ', $tables);
		$result = DB::query($query);

		$totalCount = 0;
		foreach($result as $row) {
			$record = DataObject::get_by_id($row['Source'], $row['ID']);
			if($record->canView()) {
				$results->push($record);
				$totalCount++;
			}
		}

		$results->setPageLimits($start, $pageLength, $totalCount);

		return $results;
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
	 * @param $fields array The list of field names to search on
	 * @param $keywords string The search query
	 * @param $booleanSearch A MySQL-specific flag to switch to boolean search
	 */
	function fullTextSearchSQL($fields, $keywords, $booleanSearch = false) {
		$fieldNames = '"' . implode('", "', $fields) . '"';

	 	$SQL_keywords = Convert::raw2sql($keywords);

		return "FREETEXT (($fieldNames), '$SQL_keywords')";
	}
	
	/**
	 * Remove noise words that would kill a MSSQL full-text query
	 *
	 * @param string $keywords 
	 * @return string $keywords with noise words removed
	 * @author Tom Rix
	 */
	static public function removeNoiseWords($keywords) {
		$goodWords = array();
		foreach (explode(' ', $keywords) as $word) {
			// @todo we may want to remove +'s -'s etc too
			if (!in_array($word, self::$noiseWords)) {
				$goodWords[] = $word;
			}
		}
		return join(' ', $goodWords);
	}
	
	/*
	 * Does this database support transactions?
	 */
	public function supportsTransactions(){
		return $this->supportsTransactions;
	}
	
	/*
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
	
	/*
	 * Start a prepared transaction
	 * See http://developer.postgresql.org/pgdocs/postgres/sql-set-transaction.html for details on transaction isolation options
	 */
	public function startTransaction($transaction_mode=false, $session_characteristics=false){
		//Transactions not set up for MSSQL yet
	}
	
	/*
	 * Create a savepoint that you can jump back to if you encounter problems
	 */
	public function transactionSavepoint($savepoint){
		//Transactions not set up for MSSQL yet
	}
	
	/*
	 * Rollback or revert to a savepoint if your queries encounter problems
	 * If you encounter a problem at any point during a transaction, you may
	 * need to rollback that particular query, or return to a savepoint
	 */
	public function transactionRollback($savepoint=false){
		//Transactions not set up for MSSQL yet
	}
	
	/*
	 * Commit everything inside this transaction so far
	 */
	public function endTransaction(){
		//Transactions not set up for MSSQL yet
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

		if($format == '%U') return "DATEDIFF(s, '19700101 12:00:00:000', $date)";

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
	 * Hook the result-set given into a Query class, suitable for use by sapphire.
	 * @param database The database object that created this query.
	 * @param handle the internal mssql handle that is points to the resultset.
	 */
	public function __construct(MSSQLDatabase $database, $handle, $mssql) {
		$this->database = $database;
		$this->handle = $handle;
		$this->mssql = $mssql;
	}

	public function __destroy() {
		if($this->mssql) {
			mssql_free_result($this->handle);
		} else {
			sqlsrv_free_stmt($this->handle);
		}
	}

	public function seek($row) {
		if($this->mssql) {
			return mssql_data_seek($this->handle, $row);
		} else {
			user_error("MSSQLQuery::seek() sqlsrv doesn't support seek.", E_USER_WARNING);
		}
	}

	public function numRecords() {
		if($this->mssql) {
			return mssql_num_rows($this->handle);
		} else {
			return sqlsrv_num_rows($this->handle);
		}
	}

	public function nextRecord() {
		// Coalesce rather than replace common fields.
		$output = array();

		if($this->mssql) {			
			if($data = mssql_fetch_row($this->handle)) {
				foreach($data as $columnIdx => $value) {
					$columnName = mssql_field_name($this->handle, $columnIdx);
					// There are many places in the framework that expect the ID to be a string, not a double
					// Do not set this to an integer, or it will cause failures in many tests that expect a string
					if($columnName == 'ID') $value = (string) $value;
					// $value || !$ouput[$columnName] means that the *last* occurring value is shown
					// !$ouput[$columnName] means that the *first* occurring value is shown
					if(isset($value) || !isset($output[$columnName])) {
						$output[$columnName] = $value;
					}
				}

				return $output;
			}
		} else {
			if($this->handle && $data = sqlsrv_fetch_array($this->handle, SQLSRV_FETCH_NUMERIC)) {
				$fields = sqlsrv_field_metadata($this->handle);
				foreach($fields as $columnIdx => $field) {
					$value = $data[$columnIdx];
					if($value instanceof DateTime) $value = $value->format('Y-m-d H:i:s');
					
					// $value || !$ouput[$columnName] means that the *last* occurring value is shown
					// !$ouput[$columnName] means that the *first* occurring value is shown
					if(isset($value) || !isset($output[$field['Name']])) {
						$output[$field['Name']] = $value;
					}
				}

				return $output;
			} else {
				// Free the handle if there are no more results - sqlsrv crashes if there are too many handles
				if($this->handle) {
					sqlsrv_free_stmt($this->handle);
					$this->handle = null;
				}
			}
		}

		return false;
	}

}