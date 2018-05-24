<?php

namespace SilverStripe\MSSQL;

use SilverStripe\ORM\Connect\DBSchemaManager;

/**
 * Represents and handles all schema management for a MS SQL database
 */
class MSSQLSchemaManager extends DBSchemaManager
{

    /**
     * Stores per-request cached constraint checks that come from the database.
     *
     * @var array
     */
    protected static $cached_checks = array();

    /**
     * Builds the internal MS SQL Server index name given the silverstripe table and index name
     *
     * @param string $tableName
     * @param string $indexName
     * @param string $prefix The optional prefix for the index. Defaults to "ix" for indexes.
     * @return string The name of the index
     */
    public function buildMSSQLIndexName($tableName, $indexName, $prefix = 'ix')
    {

        // Cleanup names of namespaced tables
        $tableName = str_replace('\\', '_', $tableName);
        $indexName = str_replace('\\', '_', $indexName);

        return "{$prefix}_{$tableName}_{$indexName}";
    }


    /**
     * This will set up the full text search capabilities.
     *
     * @param string $name Name of full text catalog to use
     */
    public function createFullTextCatalog($name = 'ftCatalog')
    {
        $result = $this->fullTextCatalogExists();
        if (!$result) {
            $this->query("CREATE FULLTEXT CATALOG \"$name\" AS DEFAULT;");
        }
    }

    /**
     * Check that a fulltext catalog has been created yet.
     *
     * @param string $name Name of full text catalog to use
     * @return boolean
     */
    public function fullTextCatalogExists($name = 'ftCatalog')
    {
        return (bool) $this->preparedQuery(
            "SELECT name FROM sys.fulltext_catalogs WHERE name = ?;",
            array($name)
        )->value();
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
    public function waitUntilIndexingFinished($maxWaitingTime = 15)
    {
        if (!$this->database->fullTextEnabled()) {
            return;
        }

        $this->query("EXEC sp_fulltext_catalog 'ftCatalog', 'Rebuild';");

        // Busy wait until it's done updating, but no longer than 15 seconds.
        $start = time();
        while (time() - $start < $maxWaitingTime) {
            $status = $this->query("EXEC sp_help_fulltext_catalogs 'ftCatalog';")->first();

            if (isset($status['STATUS']) && $status['STATUS'] == 0) {
                // Idle!
                break;
            }
            sleep(1);
        }
    }

    /**
     * Check if a fulltext index exists on a particular table name.
     *
     * @param string $tableName
     * @return boolean TRUE index exists | FALSE index does not exist | NULL no support
     */
    public function fulltextIndexExists($tableName)
    {
        // Special case for no full text index support
        if (!$this->database->fullTextEnabled()) {
            return null;
        }

        return (bool) $this->preparedQuery("
			SELECT 1 FROM sys.fulltext_indexes i
			JOIN sys.objects o ON i.object_id = o.object_id
			WHERE o.name = ?",
            array($tableName)
        )->value();
    }

    /**
     * MSSQL stores the primary key column with an internal identifier,
     * so a lookup needs to be done to determine it.
     *
     * @param string $tableName Name of table with primary key column "ID"
     * @return string Internal identifier for primary key
     */
    public function getPrimaryKey($tableName)
    {
        $indexes = $this->query("EXEC sp_helpindex '$tableName';");
        $indexName = '';
        foreach ($indexes as $index) {
            if ($index['index_keys'] == 'ID') {
                $indexName = $index['index_name'];
                break;
            }
        }

        return $indexName;
    }

    /**
     * Gets the identity column of a table
     *
     * @param string $tableName
     * @return string|null
     */
    public function getIdentityColumn($tableName)
    {
        return $this->preparedQuery("
			SELECT
				TABLE_NAME + '.' + COLUMN_NAME,
				TABLE_NAME
 			FROM
				INFORMATION_SCHEMA.COLUMNS
 			WHERE
				TABLE_SCHEMA = ? AND
				COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND
				TABLE_NAME = ?
		", array('dbo', $tableName))->value();
    }

    public function createDatabase($name)
    {
        $this->query("CREATE DATABASE \"$name\"");
    }

    public function dropDatabase($name)
    {
        $this->query("DROP DATABASE \"$name\"");
    }

    public function databaseExists($name)
    {
        $databases = $this->databaseList();
        foreach ($databases as $dbname) {
            if ($dbname == $name) {
                return true;
            }
        }
        return false;
    }

    public function databaseList()
    {
        return $this->query('SELECT NAME FROM sys.sysdatabases')->column();
    }

    /**
     * Create a new table.
     * @param string $tableName The name of the table
     * @param array $fields A map of field names to field types
     * @param array $indexes A map of indexes
     * @param array $options An map of additional options.  The available keys are as follows:
     *   - 'MSSQLDatabase'/'MySQLDatabase'/'PostgreSQLDatabase' - database-specific options such as "engine" for MySQL.
     *   - 'temporary' - If true, then a temporary table will be created
     * @param array $advancedOptions
     * @return string The table name generated.  This may be different from the table name, for example with temporary tables.
     */
    public function createTable($tableName, $fields = null, $indexes = null, $options = null, $advancedOptions = null)
    {
        $fieldSchemas = $indexSchemas = "";
        if ($fields) {
            foreach ($fields as $k => $v) {
                $fieldSchemas .= "\"$k\" $v,\n";
            }
        }

        // Temporary tables start with "#" in MSSQL-land
        if (!empty($options['temporary'])) {
            // Randomize the temp table name to avoid conflicts in the tempdb table which derived databases share
            $tableName = "#$tableName" . '-' . rand(1000000, 9999999);
        }

        $this->query("CREATE TABLE \"$tableName\" (
			$fieldSchemas
			primary key (\"ID\")
		);");

        //we need to generate indexes like this: CREATE INDEX IX_vault_to_export ON vault (to_export);
        //This needs to be done AFTER the table creation, so we can set up the fulltext indexes correctly
        if ($indexes) {
            foreach ($indexes as $k => $v) {
                $indexSchemas .= $this->getIndexSqlDefinition($tableName, $k, $v) . "\n";
            }
        }

        if ($indexSchemas) {
            $this->query($indexSchemas);
        }

        return $tableName;
    }

    /**
     * Alter a table's schema.
     * @param string $tableName The name of the table to alter
     * @param array $newFields New fields, a map of field name => field schema
     * @param array $newIndexes New indexes, a map of index name => index type
     * @param array $alteredFields Updated fields, a map of field name => field schema
     * @param array $alteredIndexes Updated indexes, a map of index name => index type
     * @param array $alteredOptions
     * @param array $advancedOptions
     */
    public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions=null, $advancedOptions=null)
    {
        $alterList = array();

        // drop any fulltext indexes that exist on the table before altering the structure
        if ($this->fulltextIndexExists($tableName)) {
            $alterList[] = "\nDROP FULLTEXT INDEX ON \"$tableName\";";
        }

        if ($newFields) {
            foreach ($newFields as $k => $v) {
                $alterList[] = "ALTER TABLE \"$tableName\" ADD \"$k\" $v";
            }
        }

        if ($alteredFields) {
            foreach ($alteredFields as $k => $v) {
                $alterList[] = $this->alterTableAlterColumn($tableName, $k, $v);
            }
        }
        if ($alteredIndexes) {
            foreach ($alteredIndexes as $k => $v) {
                $alterList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
            }
        }
        if ($newIndexes) {
            foreach ($newIndexes as $k => $v) {
                $alterList[] = $this->getIndexSqlDefinition($tableName, $k, $v);
            }
        }

        if ($alterList) {
            foreach ($alterList as $alteration) {
                if ($alteration != '') {
                    $this->query($alteration);
                }
            }
        }
    }

    /**
     * Given the table and column name, retrieve the constraint name for that column
     * in the table.
     *
     * @param string $tableName Table name column resides in
     * @param string $columnName Column name the constraint is for
     * @return string|null
     */
    public function getConstraintName($tableName, $columnName)
    {
        return $this->preparedQuery("
			SELECT CONSTRAINT_NAME
			FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
			WHERE TABLE_NAME = ? AND COLUMN_NAME = ?",
            array($tableName, $columnName)
        )->value();
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
    public function getConstraintCheckClause($tableName, $columnName)
    {
        // Check already processed table columns
        if (isset(self::$cached_checks[$tableName])) {
            if (!isset(self::$cached_checks[$tableName][$columnName])) {
                return null;
            }
            return self::$cached_checks[$tableName][$columnName];
        }

        // Regenerate cehcks for this table
        $checks = array();
        foreach ($this->preparedQuery("
			SELECT CAST(CHECK_CLAUSE AS TEXT) AS CHECK_CLAUSE, COLUMN_NAME
			FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS CC
			INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_NAME = CC.CONSTRAINT_NAME
			WHERE TABLE_NAME = ?",
            array($tableName)
        ) as $record) {
            $checks[$record['COLUMN_NAME']] = $record['CHECK_CLAUSE'];
        }
        self::$cached_checks[$tableName] = $checks;

        // Return via cached records
        return $this->getConstraintCheckClause($tableName, $columnName);
    }

    /**
     * Return the name of the default constraint applied to $tableName.$colName.
     * Will return null if no such constraint exists
     *
     * @param string $tableName Name of the table
     * @param string $colName Name of the column
     * @return string|null
     */
    protected function defaultConstraintName($tableName, $colName)
    {
        return $this->preparedQuery("
			SELECT s.name --default name
			FROM sys.sysobjects s
			join sys.syscolumns c ON s.parent_obj = c.id
			WHERE s.xtype = 'd'
			and c.cdefault = s.id
			and parent_obj = OBJECT_ID(?)
			and c.name = ?",
            array($tableName, $colName)
        )->value();
    }

    /**
     * Get enum values from a constraint check clause.
     *
     * @param string $clause Check clause to parse values from
     * @return array Enum values
     */
    protected function enumValuesFromCheckClause($clause)
    {
        $segments = preg_split('/ +OR *\[/i', $clause);
        $constraints = array();
        foreach ($segments as $segment) {
            $bits = preg_split('/ *= */', $segment);
            for ($i = 1; $i < sizeof($bits); $i += 2) {
                array_unshift($constraints, substr(rtrim($bits[$i], ')'), 1, -1));
            }
        }
        return $constraints;
    }

    /*
     * Creates an ALTER expression for a column in MS SQL
     *
     * @param string $tableName Name of the table to be altered
     * @param string $colName   Name of the column to be altered
     * @param string $colSpec   String which contains conditions for a column
     * @return string
     */
    protected function alterTableAlterColumn($tableName, $colName, $colSpec)
    {

        // First, we split the column specifications into parts
        // TODO: this returns an empty array for the following string: int(11) not null auto_increment
        //		 on second thoughts, why is an auto_increment field being passed through?
        $pattern = '/^(?<definition>[\w()]+)\s?(?<null>(?:not\s)?null)?\s?(?<default>default\s[\w\']+)?\s?(?<check>check\s?[\w()\'",\s]+)?$/i';
        $matches = array();
        preg_match($pattern, $colSpec, $matches);

        // drop the index if it exists
        $alterQueries = array();

        // drop *ALL* indexes on a table before proceeding
        // this won't drop primary keys, though
        $indexes = $this->indexNames($tableName);
        $indexes = array_filter($indexes);

        foreach ($indexes as $indexName) {
            $alterQueries[] = "IF EXISTS (SELECT name FROM sys.indexes WHERE name = '$indexName' AND object_id = object_id(SCHEMA_NAME() + '.$tableName')) DROP INDEX \"$indexName\" ON \"$tableName\";";
        }

        $prefix = "ALTER TABLE \"$tableName\" ";

        // Remove the old default prior to adjusting the column.
        if ($defaultConstraintName = $this->defaultConstraintName($tableName, $colName)) {
            $alterQueries[] = "$prefix DROP CONSTRAINT \"$defaultConstraintName\";";
        }

        if (isset($matches['definition'])) {
            //We will prevent any changes being made to the ID column.  Primary key indexes will have a fit if we do anything here.
            if ($colName != 'ID') {

                // SET null / not null
                $nullFragment = empty($matches['null']) ? '' : " {$matches['null']}";
                $alterQueries[] = "$prefix ALTER COLUMN \"$colName\" {$matches['definition']}$nullFragment;";

                // Add a default back
                if (!empty($matches['default'])) {
                    $alterQueries[] = "$prefix ADD {$matches['default']} FOR \"$colName\";";
                }

                // SET check constraint (The constraint HAS to be dropped)
                if (!empty($matches['check'])) {
                    $constraint = $this->getConstraintName($tableName, $colName);
                    if ($constraint) {
                        $alterQueries[] = "$prefix DROP CONSTRAINT {$constraint};";
                    }

                    //NOTE: 'with nocheck' seems to solve a few problems I've been having for modifying existing tables.
                    $alterQueries[] = "$prefix WITH NOCHECK ADD CONSTRAINT \"{$tableName}_{$colName}_check\" {$matches['check']};";
                }
            }
        }

        return implode("\n", $alterQueries);
    }

    public function renameTable($oldTableName, $newTableName)
    {
        $this->query("EXEC sp_rename \"$oldTableName\", \"$newTableName\"");
    }

    /**
     * Checks a table's integrity and repairs it if necessary.
     * NOTE: MSSQL does not appear to support any vacuum or optimise commands
     *
     * @var string $tableName The name of the table.
     * @return boolean Return true if the table has integrity after the method is complete.
     */
    public function checkAndRepairTable($tableName)
    {
        return true;
    }

    public function createField($tableName, $fieldName, $fieldSpec)
    {
        $this->query("ALTER TABLE \"$tableName\" ADD \"$fieldName\" $fieldSpec");
    }

    /**
     * Change the database type of the given field.
     * @param string $tableName The name of the tbale the field is in.
     * @param string $fieldName The name of the field to change.
     * @param string $fieldSpec The new field specification
     */
    public function alterField($tableName, $fieldName, $fieldSpec)
    {
        $this->query("ALTER TABLE \"$tableName\" CHANGE \"$fieldName\" \"$fieldName\" $fieldSpec");
    }

    public function renameField($tableName, $oldName, $newName)
    {
        $this->query("EXEC sp_rename @objname = '$tableName.$oldName', @newname = '$newName', @objtype = 'COLUMN'");
    }

    public function fieldList($table)
    {
        //This gets us more information than we need, but I've included it all for the moment....
        $fieldRecords = $this->preparedQuery("SELECT ordinal_position, column_name, data_type, column_default,
			is_nullable, character_maximum_length, numeric_precision, numeric_scale, collation_name
			FROM information_schema.columns WHERE table_name = ?
			ORDER BY ordinal_position;",
            array($table)
        );

        // Cache the records from the query - otherwise a lack of multiple active result sets
        // will cause subsequent queries to fail in this method
        $fields = array();
        $output = array();
        foreach ($fieldRecords as $record) {
            $fields[] = $record;
        }

        foreach ($fields as $field) {
            // Update the data_type field to be a complete column definition string for use by
            // SS_Database::requireField()
            switch ($field['data_type']) {
                case 'int':
                case 'bigint':
                case 'numeric':
                case 'float':
                case 'bit':
                    if ($field['data_type'] != 'bigint' && $field['data_type'] != 'int' && $sizeSuffix = $field['numeric_precision']) {
                        $field['data_type'] .= "($sizeSuffix)";
                    }

                    if ($field['is_nullable'] == 'YES') {
                        $field['data_type'] .= ' null';
                    } else {
                        $field['data_type'] .= ' not null';
                    }
                    if ($field['column_default']) {
                        $default=substr($field['column_default'], 2, -2);
                        $field['data_type'] .= " default $default";
                    }
                    break;

                case 'decimal':
                    if ($field['numeric_precision']) {
                        $sizeSuffix = $field['numeric_precision'] . ',' . $field['numeric_scale'];
                        $field['data_type'] .= "($sizeSuffix)";
                    }

                    if ($field['is_nullable'] == 'YES') {
                        $field['data_type'] .= ' null';
                    } else {
                        $field['data_type'] .= ' not null';
                    }
                    if ($field['column_default']) {
                        $default=substr($field['column_default'], 2, -2);
                        $field['data_type'] .= " default $default";
                    }
                    break;

                case 'nvarchar':
                case 'varchar':
                    //Check to see if there's a constraint attached to this column:
                    $clause = $this->getConstraintCheckClause($table, $field['column_name']);
                    if ($clause) {
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
                    if ($sizeSuffix == '-1') {
                        $sizeSuffix = 'max';
                    }
                    if ($sizeSuffix) {
                        $field['data_type'] .= "($sizeSuffix)";
                    }

                    if ($field['is_nullable'] == 'YES') {
                        $field['data_type'] .= ' null';
                    } else {
                        $field['data_type'] .= ' not null';
                    }
                    if ($field['column_default']) {
                        $default=substr($field['column_default'], 2, -2);
                        $field['data_type'] .= " default '$default'";
                    }
            }
            $output[$field['column_name']] = $field;
        }

        return $output;
    }

    /**
     * Create an index on a table.
     * @param string $tableName The name of the table.
     * @param string $indexName The name of the index.
     * @param string $indexSpec The specification of the index, see SS_Database::requireIndex() for more details.
     */
    public function createIndex($tableName, $indexName, $indexSpec)
    {
        $this->query($this->getIndexSqlDefinition($tableName, $indexName, $indexSpec));
    }

    /**
     * Return SQL for dropping and recreating an index
     *
     * @param string $tableName Name of table to create this index against
     * @param string $indexName Name of this index
     * @param array|string $indexSpec Index specification, either as a raw string
     * or parsed array form
     * @return string The SQL required to generate this index
     */
    protected function getIndexSqlDefinition($tableName, $indexName, $indexSpec)
    {

        // Determine index name
        $index = $this->buildMSSQLIndexName($tableName, $indexName);

        // Consolidate/Cleanup spec into array format
        $columns = $this->implodeColumnList($indexSpec['columns']);

        $drop = "IF EXISTS (SELECT name FROM sys.indexes WHERE name = '$index' AND object_id = object_id(SCHEMA_NAME() + '.$tableName')) DROP INDEX $index ON \"$tableName\";";

        // create a type-specific index
        if ($indexSpec['type'] == 'fulltext') {
            if(!$this->database->fullTextEnabled()) {
                return '';
            }
            // enable fulltext on this table
            $this->createFullTextCatalog();
            $primary_key = $this->getPrimaryKey($tableName);

            if ($primary_key) {
                return "$drop CREATE FULLTEXT INDEX ON \"$tableName\" ({$columns})"
                     . "KEY INDEX $primary_key WITH CHANGE_TRACKING AUTO;";
            }
        }

        if ($indexSpec['type'] == 'unique') {
            return "$drop CREATE UNIQUE INDEX $index ON \"$tableName\" ({$columns});";
        }

        return "$drop CREATE INDEX $index ON \"$tableName\" ({$columns});";
    }

    public function alterIndex($tableName, $indexName, $indexSpec)
    {
        $this->createIndex($tableName, $indexName, $indexSpec);
    }

    /**
     * Return the list of indexes in a table.
     * @param string $table The table name.
     * @return array
     */
    public function indexList($table)
    {
        $indexes = $this->query("EXEC sp_helpindex '$table';");
        $indexList = array();

        // Enumerate all basic indexes
        foreach ($indexes as $index) {
            if (strpos($index['index_description'], 'unique') !== false) {
                $indexType = 'unique ';
            } else {
                $indexType = 'index ';
            }

            // Extract name from index
            $baseIndexName = $this->buildMSSQLIndexName($table, '');
            $indexName = substr($index['index_name'], strlen($baseIndexName));

            // Extract columns
            $columns = $this->quoteColumnSpecString($index['index_keys']);
            $indexList[$indexName] = array(
                'name' => $indexName,
                'columns' => $this->explodeColumnString($columns),
                'type' => $indexType
            );
        }

        // Now we need to check to see if we have any fulltext indexes attached to this table:
        if ($this->database->fullTextEnabled()) {
            $result = $this->query('EXEC sp_help_fulltext_columns;');

            // Extract columns from this fulltext definition
            $columns = array();
            foreach ($result as $row) {
                if ($row['TABLE_NAME'] == $table) {
                    $columns[] = $row['FULLTEXT_COLUMN_NAME'];
                }
            }

            if (!empty($columns)) {
                $indexList['SearchFields'] = array(
                    'name' => 'SearchFields',
                    'columns' => $this->implodeColumnList($columns),
                    'type' => 'fulltext'
                );
            }
        }

        return $indexList;
    }

    /**
     * For a given table name, get all the internal index names,
     * except for those that are primary keys and fulltext indexes.
     *
     * @param string $tableName
     * @return array
     */
    public function indexNames($tableName)
    {
        return $this->preparedQuery('
			SELECT ind.name FROM sys.indexes ind
			INNER JOIN sys.tables t ON ind.object_id = t.object_id
			WHERE is_primary_key = 0 AND t.name = ? AND ind.name IS NOT NULL',
            array($tableName)
        )->column();
    }

    public function tableList()
    {
        $tables = array();
        foreach ($this->query("EXEC sp_tables @table_owner = 'dbo';") as $record) {
            $tables[strtolower($record['TABLE_NAME'])] = $record['TABLE_NAME'];
        }
        return $tables;
    }

    /**
     * Return a boolean type-formatted string
     * We use 'bit' so that we can do numeric-based comparisons
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function boolean($values)
    {
        $default = ($values['default']) ? '1' : '0';
        return 'bit not null default ' . $default;
    }

    /**
     * Return a date type-formatted string.
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function date($values)
    {
        return 'date null';
    }

    /**
     * Return a decimal type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function decimal($values)
    {
        // Avoid empty strings being put in the db
        if ($values['precision'] == '') {
            $precision = 1;
        } else {
            $precision = $values['precision'];
        }

        $defaultValue = '0';
        if (isset($values['default']) && is_numeric($values['default'])) {
            $defaultValue = $values['default'];
        }

        return "decimal($precision) not null default $defaultValue";
    }

    /**
     * Return a enum type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function enum($values)
    {
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
     *
     * @param array $values
     * @return string
     */
    public function set($values)
    {
        return $this->enum($values);
    }

    /**
     * Return a float type-formatted string.
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function float($values)
    {
        return 'float(53) not null default ' . $values['default'];
    }

    /**
     * Return a int type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function int($values)
    {
        return 'int not null default ' . (int) $values['default'];
    }

    /**
     * Return a bigint type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function bigint($values)
    {
        return 'bigint not null default ' . (int) $values['default'];
    }

    /**
     * Return a datetime type-formatted string
     * For MS SQL, we simply return the word 'timestamp', no other parameters are necessary
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function datetime($values)
    {
        return 'datetime null';
    }

    /**
     * Return a text type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function text($values)
    {
        $collation = MSSQLDatabase::get_collation();
        $collationSQL = $collation ? " COLLATE $collation" : "";
        return "nvarchar(max)$collationSQL null";
    }

    /**
     * Return a time type-formatted string.
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function time($values)
    {
        return 'time null';
    }

    /**
     * Return a varchar type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function varchar($values)
    {
        $collation = MSSQLDatabase::get_collation();
        $collationSQL = $collation ? " COLLATE $collation" : "";
        return "nvarchar(" . $values['precision'] . ")$collationSQL null";
    }

    /**
     * Return a 4 digit numeric type.
     *
     * @param array $values
     * @return string
     */
    public function year($values)
    {
        return 'numeric(4)';
    }

    /**
     * This returns the column which is the primary key for each table
     *
     * @param bool $asDbValue
     * @param bool $hasAutoIncPK
     * @return string
     */
    public function IdColumn($asDbValue = false, $hasAutoIncPK = true)
    {
        if ($asDbValue) {
            return 'int not null';
        } elseif ($hasAutoIncPK) {
            return 'int identity(1,1)';
        } else {
            return 'int not null';
        }
    }

    public function hasTable($tableName)
    {
        return (bool)$this->preparedQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            array($tableName)
        )->value();
    }

    /**
     * Returns the values of the given enum field
     * NOTE: Experimental; introduced for db-abstraction and may changed before 2.4 is released.
     *
     * @param string $tableName
     * @param string $fieldName
     * @return array
     */
    public function enumValuesForField($tableName, $fieldName)
    {
        $classes = array();

        // Get the enum of all page types from the SiteTree table
        $clause = $this->getConstraintCheckClause($tableName, $fieldName);
        if ($clause) {
            $classes = $this->enumValuesFromCheckClause($clause);
        }

        return $classes;
    }

    /**
     * This is a lookup table for data types.
     *
     * For instance, MSSQL uses 'BIGINT', while MySQL uses 'UNSIGNED'
     * and PostgreSQL uses 'INT'.
     *
     * @param string $type
     * @return string
     */
    public function dbDataType($type)
    {
        $values = array(
            'unsigned integer'=>'BIGINT'
        );
        if (isset($values[$type])) {
            return $values[$type];
        } else {
            return '';
        }
    }

    protected function indexKey($table, $index, $spec)
    {
        return $index;
    }
}
