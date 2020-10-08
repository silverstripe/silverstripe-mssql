<?php

namespace SilverStripe\MSSQL;

use SilverStripe\ORM\Connect\DBConnector;
use function sqlsrv_connect;
use function sqlsrv_begin_transaction;
use function sqlsrv_rollback;
use function sqlsrv_query;

use const MSSQL_USE_WINDOWS_AUTHENTICATION;

/**
 * Database connector driver for sqlsrv_ library
 */
class SQLServerConnector extends DBConnector
{

    /**
     * Connection to the DBMS.
     *
     * @var resource
     */
    protected $dbConn = null;

    /**
     * Stores the affected rows of the last query.
     * Used by sqlsrv functions only, as sqlsrv_rows_affected
     * accepts a result instead of a database handle.
     *
     * @var integer
     */
    protected $lastAffectedRows;

    /**
     * Name of the currently selected database
     *
     * @var string
     */
    protected $selectedDatabase = null;

    public function connect($parameters, $selectDB = false)
    {
        // Disable default warnings as errors behaviour for sqlsrv to keep it in line with mssql functions
        if (ini_get('sqlsrv.WarningsReturnAsErrors')) {
            ini_set('sqlsrv.WarningsReturnAsErrors', 'Off');
        }

        $charset = isset($parameters['charset']) ? $parameters : 'UTF-8';
        $multiResultSets = isset($parameters['multipleactiveresultsets'])
                ? $parameters['multipleactiveresultsets']
                : true;
        $options = array(
            'CharacterSet' => $charset,
            'MultipleActiveResultSets' => $multiResultSets
        );

        if (!(defined('MSSQL_USE_WINDOWS_AUTHENTICATION') && MSSQL_USE_WINDOWS_AUTHENTICATION == true)
            && empty($parameters['windowsauthentication'])
        ) {
            $options['UID'] = $parameters['username'];
            $options['PWD'] = $parameters['password'];
        }

        // Required by MS Azure database
        if ($selectDB && !empty($parameters['database'])) {
            $options['Database'] = $parameters['database'];
        }

        $this->dbConn = sqlsrv_connect($parameters['server'], $options);

        if (empty($this->dbConn)) {
            var_dump(sqlsrv_errors());
            $this->databaseError("Couldn't connect to SQL Server database");
        } elseif ($selectDB && !empty($parameters['database'])) {
            // Check selected database (Azure)
            $this->selectedDatabase = $parameters['database'];
        }
    }

    /**
     * Start transaction. READ ONLY not supported.
     */
    public function transactionStart()
    {
        $result = sqlsrv_begin_transaction($this->dbConn);
        if (!$result) {
            $this->databaseError("Couldn't start the transaction.");
        }
    }

    /**
     * Commit everything inside this transaction so far
     */
    public function transactionEnd()
    {
        $result = sqlsrv_commit($this->dbConn);
        if (!$result) {
            $this->databaseError("Couldn't commit the transaction.");
        }
    }

    /**
     * Rollback or revert to a savepoint if your queries encounter problems
     * If you encounter a problem at any point during a transaction, you may
     * need to rollback that particular query, or return to a savepoint
     */
    public function transactionRollback()
    {
        $result = sqlsrv_rollback($this->dbConn);
        if (!$result) {
            $this->databaseError("Couldn't rollback the transaction.");
        }
    }

    public function affectedRows()
    {
        return $this->lastAffectedRows;
    }

    public function getLastError()
    {
        $errorMessages = array();
        $errors = sqlsrv_errors();
        if ($errors) {
            foreach ($errors as $info) {
                $errorMessages[] = implode(', ', array($info['SQLSTATE'], $info['code'], $info['message']));
            }
        }
        return implode('; ', $errorMessages);
    }

    public function isActive()
    {
        return $this->dbConn && $this->selectedDatabase;
    }

    public function preparedQuery($sql, $parameters, $errorLevel = E_USER_ERROR)
    {
        // Reset state
        $this->lastAffectedRows = 0;

        // Run query
        $parsedParameters = $this->parameterValues($parameters);
        if (empty($parsedParameters)) {
            $handle = sqlsrv_query($this->dbConn, $sql);
        } else {
            $handle = sqlsrv_query($this->dbConn, $sql, $parsedParameters);
        }

        // Check for error
        if (!$handle) {
            $this->databaseError($this->getLastError(), $errorLevel, $sql, $parsedParameters);
            return null;
        }

        // Report result
        $this->lastAffectedRows = sqlsrv_rows_affected($handle);
        return new SQLServerQuery($this, $handle);
    }

    public function query($sql, $errorLevel = E_USER_ERROR)
    {
        return $this->preparedQuery($sql, array(), $errorLevel);
    }

    public function selectDatabase($name)
    {
        $this->query("USE \"$name\"");
        $this->selectedDatabase = $name;
        return true;
    }

    public function __destruct()
    {
        if (is_resource($this->dbConn)) {
            sqlsrv_close($this->dbConn);
        }
    }

    public function getVersion()
    {
        // @todo - use sqlsrv_server_info?
        return trim($this->query("SELECT CONVERT(char(15), SERVERPROPERTY('ProductVersion'))")->value());
    }

    public function getGeneratedID($table)
    {
        return $this->query("SELECT IDENT_CURRENT('$table')")->value();
    }

    public function getSelectedDatabase()
    {
        return $this->selectedDatabase;
    }

    public function unloadDatabase()
    {
        $this->selectDatabase('Master');
        $this->selectedDatabase = null;
    }

    /**
     * Quotes a string, including the "N" prefix so unicode
     * strings are saved to the database correctly.
     *
     * @param string $value String to be encoded
     * @return string Processed string ready for DB
     */
    public function quoteString($value)
    {
        return "N'" . $this->escapeString($value) . "'";
    }

    public function escapeString($value)
    {
        $value = str_replace("'", "''", $value);
        $value = str_replace("\0", "[NULL]", $value);
        return $value;
    }
}
