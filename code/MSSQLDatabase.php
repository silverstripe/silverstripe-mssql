<?php
/**
 * Microsoft SQL Server connector class.
 * 
 * @package sapphire
 * @subpackage model
 */
class MSSQLDatabase extends Database {
	
	/**
	 * Connection to the DBMS.
	 * @var resource
	 */
	private $dbConn;
	
	/**
	 * True if we are connected to a database.
	 * @var boolean
	 */
	private $active;
	
	/**
	 * The name of the database.
	 * @var string
	 */
	private $database;
	
	/**
	 * Does this database have full-text index supprt
	 */
	protected $fullTextEnabled = true;
	
	/**
	 * If true, use the mssql_... functions.
	 * If false use the sqlsrv_... functions
	 */
	private $mssql = null;

	/**
	 * Sorts the last query's affected row count, for sqlsrv module only.
	 * @todo This is a bit clumsy; affectedRows() should be moved to {@link Query} object, so that this isn't necessary.
	 */
	private $lastAffectedRows;
	
	/**
	 * The version of MSSQL
	 * @var float
	 */
	private $mssqlVersion;
	
	/**
	 * Words that will trigger an error if passed to a SQL Server fulltext search
	 */
	public static $noiseWords = array("about", "1", "after", "2", "all", "also", "3", "an", "4", "and", "5", "another", "6", "any", "7", "are", "8", "as", "9", "at", "0", "be", "$", "because", "been", "before", "being", "between", "both", "but", "by", "came", "can", "come", "could", "did", "do", "does", "each", "else", "for", "from", "get", "got", "has", "had", "he", "have", "her", "here", "him", "himself", "his", "how", "if", "in", "into", "is", "it", "its", "just", "like", "make", "many", "me", "might", "more", "most", "much", "must", "my", "never", "no", "now", "of", "on", "only", "or", "other", "our", "out", "over", "re", "said", "same", "see", "should", "since", "so", "some", "still", "such", "take", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "up", "use", "very", "want", "was", "way", "we", "well", "were", "what", "when", "where", "which", "while", "who", "will", "with", "would", "you", "your", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
	
	/**
	 * Connect to a MS SQL database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - server: The server, eg, localhost
	 *  - username: The username to log on with
	 *  - password: The password to log on with
	 *  - database: The database to connect to
	 */
	public function __construct($parameters) {
		parent::__construct();
		
		if(function_exists('mssql_connect')) {
			$this->mssql = true;
		} else if(function_exists('sqlsrv_connect')) {
			$this->mssql = false;
		} else {
			user_error("Neither the mssql_connect() nor the sqlsrv_connect() functions are available.  Please install the PHP native mssql module, or the Microsoft-provided sqlsrv module.", E_USER_ERROR);
		}
		
		if($this->mssql) {
			$this->dbConn = mssql_connect($parameters['server'], $parameters['username'], $parameters['password']);
		} else {
			$this->dbConn = sqlsrv_connect($parameters['server'], array(
				'UID' => $parameters['username'],
				'PWD' => $parameters['password'],
			));
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
		if($this->mssql) {
			mssql_close($this->dbConn);
		} else {
			sqlsrv_close($this->dbConn);
		}
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
	 * Theoretically, you don't need to 'enable' it every time...
	 *
	 * TODO: make this a _config.php setting
	 * TODO: VERY IMPORTANT: move this so it only gets called upon a dev/build action
	 */
	function createFullTextCatalog(){
		if($this->fullTextEnabled) {
			$this->query("exec sp_fulltext_database 'enable';");
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
				
		if($this->mssql) {
			$handle = mssql_query($sql, $this->dbConn);
		} else {
			$handle = sqlsrv_query($this->dbConn, $sql);
		}
		
		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			Debug::message("\n$sql\n{$endtime}ms\n", false);
		}
		
		DB::$lastQuery=$handle;
		
		if(!$handle && $errorLevel) $this->databaseError("Couldn't run query: $sql", $errorLevel);
		
		if (!$this->mssql) {
			$this->lastAffectedRows = sqlsrv_rows_affected($handle);
		}
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
		
		$this->tableList = $this->fieldList = $this->indexList = null;
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
	public function createTable($tableName, $fields = null, $indexes = null, $options = null) {
		$fieldSchemas = $indexSchemas = "";
		if($fields) foreach($fields as $k => $v) $fieldSchemas .= "\"$k\" $v,\n";
		
		// Temporary tables start with "#" in MSSQL-land
		if(!empty($options['temporary'])) $tableName = "#$tableName";
		
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
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null) {
		$fieldSchemas = $indexSchemas = "";
		
		$alterList = array();
		if($newFields) foreach($newFields as $k => $v) $alterList[] .= "ALTER TABLE \"$tableName\" ADD \"$k\" $v";
		
		$indexList=$this->IndexList($tableName);
		
		if($alteredFields) {
			foreach($alteredFields as $k => $v) {				
				$val=$this->alterTableAlterColumn($tableName, $k, $v, $indexList);
				if($val!='')
					$alterList[] .= $val;
			}
		}
		
		//DB ABSTRACTION: we need to change the constraints to be a separate 'add' command,
		$alterIndexList=Array();
		if($alteredIndexes) foreach($alteredIndexes as $v) {
			//TODO: I don't think that these drop index commands will work:
			if($v['type']!='fulltext'){
				if(is_array($v))
					$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower($v['value']) . ' ON ' . $tableName . ';';
				else
					$alterIndexList[] = 'DROP INDEX ix_' . strtolower($tableName) . '_' . strtolower(trim($v, '()')) . ' ON ' . $tableName . ';';
							
				if(is_array($v))
					$k=$v['value'];
				else $k=trim($v, '()');
				
				$alterIndexList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
			}
 		}
 		
 		//Add the new indexes:
 		if($newIndexes) foreach($newIndexes as $k=>$v){
 			$alterIndexList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
 		}

 		if($alterList) {
			foreach($alterList as $this_alteration){
				if($this_alteration!=''){
					$this->query($this_alteration);
				}
			}
		}
		
		foreach($alterIndexList as $alteration) {
			if($alteration!='') $this->query($alteration);
		}
	}
	
	/**
	 * This is a private MSSQL-only function which returns
	 * specific details about a column's constraints (if any)
	 * @param string $tableName Name of table the column exists in
	 * @param string $columnName Name of column to check for
	 */
	private function ColumnConstraints($tableName, $columnName) {
		$constraint = $this->query("SELECT CC.CONSTRAINT_NAME, CAST(CHECK_CLAUSE AS TEXT) AS CHECK_CLAUSE, COLUMN_NAME FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS CC INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME=CC.CONSTRAINT_NAME WHERE TABLE_NAME='$tableName' AND COLUMN_NAME='" . $columnName . "';")->first();
		return $constraint;
	}

	/**
	 * Return the name of the default constraint applied to $tableName.$colName.
	 * Will return null if no such constraint exists
	 */
	private function defaultConstraintName($tableName, $colName) {
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
	private function EnumValuesFromConstraint($constraint){
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
	private function alterTableAlterColumn($tableName, $colName, $colSpec, $indexList){

		// First, we split the column specifications into parts
		// TODO: this returns an empty array for the following string: int(11) not null auto_increment
		//		 on second thoughts, why is an auto_increment field being passed through?
		
		$pattern = '/^([\w()]+)\s?((?:not\s)?null)?\s?(default\s[\w\']+)?\s?(check\s?[\w()\'",\s]+)?$/i';
		$matches=Array();
		preg_match($pattern, $colSpec, $matches);
		
		//if($matches[1]=='serial8')
		//	return '';
		
		//drop the index if it exists:
		$alterCol='';
		if(isset($indexList[$colName])){
			$alterCol = "\nDROP INDEX \"$tableName\".ix_{$tableName}_{$colName};";
		}

		$prefix="ALTER TABLE \"" . $tableName . "\" ";

		// Remove the old default prior to adjusting the column.
		if($defaultConstraintName = $this->defaultConstraintName($tableName, $colName)) {
			$alterCol .= ";\n$prefix DROP CONSTRAINT \"$defaultConstraintName\"";
		}
		
		if(isset($matches[1])) {
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
		$fields = $this->query("SELECT ordinal_position, column_name, data_type, column_default, 
			is_nullable, character_maximum_length, numeric_precision, numeric_scale
			FROM information_schema.columns WHERE table_name = '$table' 
			ORDER BY ordinal_position;");
		
		$output = array();
		if($fields) foreach($fields as $field) {
			// Update the data_type field to be a complete column definition string for use by
			// Database::requireField()
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
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
	 */
	public function createIndex($tableName, $indexName, $indexSpec) {
		$this->query($this->getIndexSqlDefinition($tableName, $indexName, $indexSpec));
	}
	
	/*
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
	    
		if(!is_array($indexSpec)){
			$indexSpec=trim($indexSpec, '()');
			$bits=explode(',', $indexSpec);
			$indexes="\"" . implode("\",\"", $bits) . "\"";
			
			return 'create index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (" . $indexes . ");";
		} else {
			//create a type-specific index
			if($indexSpec['type']=='fulltext'){
				if($this->fullTextEnabled) {
					//Enable full text search.
					$this->createFullTextCatalog();
				
					$primary_key=$this->getPrimaryKey($tableName);
				
					//First, we need to see if a full text search already exists:
					$result=$this->query("SELECT object_id FROM sys.fulltext_indexes WHERE object_id=object_id('$tableName');")->first();
				
					$drop='';
					if($result)
						$drop="DROP FULLTEXT INDEX ON \"" . $tableName . "\";";
				
					return $drop . "CREATE FULLTEXT INDEX ON \"$tableName\"	({$indexSpec['value']})	" .
						"KEY INDEX $primary_key WITH CHANGE_TRACKING AUTO;";
				}
			
			}
									
			if($indexSpec['type']=='unique')
				return 'create unique index ix_' . $tableName . '_' . $indexName . " ON \"" . $tableName . "\" (\"" . $indexSpec['value'] . "\");";
		}
		
	}
	
	function getDbSqlDefinition($tableName, $indexName, $indexSpec){
		return $indexName;
	}
	
	/**
	 * Alter an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
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
		if($this->fullTextEnabled) {
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
	 * A function to return the field names and datatypes for the particular table
	 */
	public function tableDetails($tableName){
		user_error("tableDetails not implemented", E_USER_WARNING);
		return array();
		/*
		$query="SELECT a.attname as \"Column\", pg_catalog.format_type(a.atttypid, a.atttypmod) as \"Datatype\" FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = ( SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^($tableName)$' AND pg_catalog.pg_table_is_visible(c.oid));";
		$result=DB::query($query);
		
		$table=Array();
		while($row=pg_fetch_assoc($result)){
			$table[]=Array('Column'=>$row['Column'], 'DataType'=>$row['DataType']);
		}
		
		return $table;
		*/
	}
	
	/**
	 * Return a boolean type-formatted string
	 * We use 'bit' so that we can do numeric-based comparisons
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function boolean($values, $asDbValue=false){
		//Annoyingly, we need to do a good ol' fashioned switch here:
		($values['default']) ? $default='1' : $default='0';
		
		if($asDbValue) {
			return array(
				'data_type'=>'bit', 
				'default' => $default
			);
		} else {
			return 'bit not null default ' . $default;
		}
	}
	
	/**
	 * Return a date type-formatted string.
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values, $asDbValue=false){
		if($asDbValue) {
			return array(
				'data_type' => 'decimal',
			);
		} else {
			return 'datetime null';
		}
	}
	
	/**
	 * Return a decimal type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function decimal($values, $asDbValue=false){
		// Avoid empty strings being put in the db
		if($values['precision'] == '') {
			$precision = 1;
		} else {
			$precision = $values['precision'];
		}
		
		if($asDbValue)
			return Array('data_type'=>'decimal', 'numeric_precision'=>'9,2');
		else return 'decimal(' . $precision . ') not null default 0';
	}
	
	/**
	 * Return a enum type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function enum($values, $asDbValue=false){
		// Enums are a bit different. We'll be creating a varchar(255) with a constraint of all the
		// usual enum options.
		// NOTE: In this one instance, we are including the table name in the values array

		$maxLength = max(array_map('strlen', $values['enums']));

		if($asDbValue) {
			return array(
				'data_type'=>'varchar', 
				'default' => $values['default'],
				'character_maximum_length'=>$maxLength,
			);
		} else {
			return "varchar($maxLength) not null default '" . $values['default'] 
				. "' check(\"" . $values['name'] . "\" in ('" . implode("','", $values['enums']) 
				. "'))";
		}
	}
	
	/**
	 * Return a float type-formatted string.
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function float($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'float');
		else return 'float';
	}
	
	/**
	 * Return a int type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values, $asDbValue=false){
		//We'll be using an 8 digit precision to keep it in line with the serial8 datatype for ID columns
		if($asDbValue)
			return Array('data_type'=>'numeric', 'numeric_precision'=>'8', 'default'=>(int)$values['default']);
		else
			return 'numeric(8) not null default ' . (int)$values['default'];
	}
	
	/**
	 * Return a datetime type-formatted string
	 * For MS SQL, we simply return the word 'timestamp', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function ssdatetime($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'datetime');
		else
			return 'datetime null';
	}
	
	/**
	 * Return a text type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function text($values, $asDbValue=false){
		if($asDbValue) {
			return array(
				'data_type'=>'varchar',
				'character_maximum_length' => -1,
			);
		} else {
			return 'varchar(max) null';
		}
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
	public function varchar($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'varchar', 'character_maximum_length'=>$values['precision']);
		else
			return 'varchar(' . $values['precision'] . ') null';
	}
	
	/**
	 * Return a 4 digit numeric type.
	 * @return string
	 */
	public function year($values, $asDbValue=false){
		if($asDbValue)
			return Array('data_type'=>'numeric', 'numeric_precision'=>'4');
		else return 'numeric(4)'; 
	}
	
	function escape_character($escape=false){
		if($escape)
			return "\\\"";
		else
			return "\"";
	}
	
	/**
	 * Create a fulltext search datatype for MSSQL.
	 *
	 * @param array $spec
	 */
	function fulltext($table, $spec){
		//$spec['name'] is the column we've created that holds all the words we want to index.
		//This is a coalesced collection of multiple columns if necessary
		//$spec='create index ix_' . $table . '_' . $spec['name'] . ' on ' . $table . ' using gist(' . $spec['name'] . ');';
		
		//return $spec;
		echo '<span style="color: Red">full text just got called!</span><br>';
		return '';
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
		return "SELECT name FROM {$this->database}..sysobjects WHERE xtype = 'U';";
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
	 * Because NOW() doesn't always work...
	 * MSSQL, I'm looking at you
	 *
	 */
	function now(){
		return 'CURRENT_TIMESTAMP';
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
	
	/*
	 * This changes the index name depending on database requirements.
	 * MSSQL requires commas to be replaced with underscores 
	 */
	function modifyIndex($index){
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
		if($this->fullTextEnabled) {
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
			$result=DB::query('EXEC sp_help_fulltext_columns');
			if (!$result->numRecords()) throw Exception('there are no full text columns to search');
			$tables= array();
			
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

			$totalCount = 0;
			$this->forceNumRows = true;
			foreach($tables as $q) {
				$qR = DB::query($q);
				$totalCount += $qR->numRecords();
			}
			$this->forceNumRows = false;
			
			//We'll do a union query on all of these tables... it's easier!
			$query=implode(' UNION ', $tables);
			
			$result=DB::query($query);
			$searchResults=new DataObjectSet();
			
			foreach($result as $row){
				$row_result=DataObject::get_by_id($row['Source'], $row['ID']);
				$searchResults->push($row_result);
			}
			
			$searchResults->setPageLimits($start, $pageLength, $totalCount);
		}

		return $searchResults;
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
}

/**
 * A result-set from a MSSQL database.
 * @package sapphire
 * @subpackage model
 */
class MSSQLQuery extends Query {
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
		
		parent::__construct();
	}
	
	public function __destroy() {
		if($this->mssql) {
			mssql_free_result($this->handle);
		} else {
			sqlsrv_free_stmt($this->handle);
		}
	}
	
	/**
	 * Please see the comments below for numRecords
	 */
	public function seek($row) {
		if($this->mssql) {
			return mssql_data_seek($this->handle, $row);
		} else {
			user_error("MSSQLQuery::seek() sqlserv doesn't support seek.", E_USER_WARNING);
		}
	}
	
	/*
	 * If we're running the sqlsrv set of functions, then the dataobject set is a forward-only cursor
	 * Therefore, we do not have access to the number of rows that this result contains
	 * This is (usually) called from Database::rewind(), which in turn seems to be called when a foreach...
	 * is started on a recordset
	 *
	 * If you are using SQLSRV, this functon will just return a true or false based on whether you got
	 * /ANY/ rows. UNLESS you set $this->forceNumRows to true, in which case, it will loop over the whole
	 * rowset, cache it, and then do the count on that. This is probably resource intensive.
	 * 
	 * For this function, and seek() (above), we will be returning false.
	 */
	public function numRecords() {
		if($this->mssql) {
			return mssql_num_rows($this->handle);
		} else {
			// Setting forceNumRows to true will cache all records, but will
			// be able to give a reliable number of results found.
			if (isset($this->forceNumRows) && $this->forceNumRows) {
				if (isset($this->numRecords)) return $this->numRecords;
				
				$this->cachedRecords = array();
				
				// We can't have nextRecord() return the row we just added =)
				$this->cachingRows = true;
						
				foreach($this as $record) {
					$this->cachedRecords[] = $record;
				}
				
				$this->cachingRows = false;
				
				// Assign it to a var, otherwise the value will change when
				// something is shifted off the beginning.
				$this->numRecords = count($this->cachedRecords);
				return $this->numRecords;
			} else {
				$this->cachedRecords = array($this->nextRecord());
				return count($this->cachedRecords[0]) ? true : false;
			}
		}
	}
	
	public function nextRecord() {
		// Coalesce rather than replace common fields.
		if($this->mssql) {			
			if($data = mssql_fetch_row($this->handle)) {
				
				foreach($data as $columnIdx => $value) {
					$columnName = mssql_field_name($this->handle, $columnIdx);
					// $value || !$ouput[$columnName] means that the *last* occurring value is shown
					// !$ouput[$columnName] means that the *first* occurring value is shown
					if(isset($value) || !isset($output[$columnName])) {
						$output[$columnName] = $value;
					}
				}
				
				return $output;
			} else {
				return false;
			}
			
		} else {
			// If we have cached rows (if numRecords as been called) and
			// returning cached rows hasn't specifically been disabled,
			// check for cached rows and return 'em.
			if (isset($this->cachedRecords) && count($this->cachedRecords) && (!isset($this->cachingRows) || !$this->cachingRows)) {
				return array_shift($this->cachedRecords);
			}
			if($data = sqlsrv_fetch_array($this->handle, SQLSRV_FETCH_NUMERIC)) {
				$output = array();
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
				return false;
			}
			
		}
	}
	
}