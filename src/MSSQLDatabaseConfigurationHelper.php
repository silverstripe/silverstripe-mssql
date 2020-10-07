<?php

namespace SilverStripe\MSSQL;

use SilverStripe\Dev\Install\DatabaseAdapterRegistry;
use SilverStripe\Dev\Install\DatabaseConfigurationHelper;
use PDO;
use Exception;

/**
 * This is a helper class for the SS installer.
 *
 * It does all the specific checking for MSSQLDatabase
 * to ensure that the configuration is setup correctly.
 */
class MSSQLDatabaseConfigurationHelper implements DatabaseConfigurationHelper
{

    protected function isAzure($databaseConfig)
    {
        /** @skipUpgrade */
        return $databaseConfig['type'] === 'MSSQLAzureDatabase';
    }

    /**
     * Create a connection of the appropriate type
     *
     * @skipUpgrade
     * @param array $databaseConfig
     * @param string $error Error message passed by value
     * @return mixed|null Either the connection object, or null if error
     */
    protected function createConnection($databaseConfig, &$error)
    {
        $error = null;
        try {
            switch ($databaseConfig['type']) {
                case 'MSSQLDatabase':
                case 'MSSQLAzureDatabase':
                    $parameters = array(
                        'UID' => $databaseConfig['username'],
                        'PWD' => $databaseConfig['password']
                    );

                    // Azure has additional parameter requirements
                    if ($this->isAzure($databaseConfig)) {
                        $parameters['database'] = $databaseConfig['database'];
                        $parameters['multipleactiveresultsets'] = 0;
                    }
                    $conn = @sqlsrv_connect($databaseConfig['server'], $parameters);
                    if ($conn) {
                        return $conn;
                    }

                    // Get error
                    if ($errors = sqlsrv_errors()) {
                        $error = '';
                        foreach ($errors as $detail) {
                            $error .= "{$detail['message']}\n";
                        }
                    } else {
                        $error = 'Unknown connection error';
                    }
                    return null;
                case 'MSSQLPDODatabase':
                    $driver = $this->getPDODriver();
                    if (!$driver) {
                        $error = 'No supported PDO driver';
                        return null;
                    }

                    // May throw a PDOException if fails
                    $conn = @new PDO($driver.':Server='.$databaseConfig['server'], $databaseConfig['username'], $databaseConfig['password']);
                    if ($conn) {
                        return $conn;
                    } else {
                        $error = 'Unknown connection error';
                        return null;
                    }
                default:
                    $error = 'Invalid connection type: ' . $databaseConfig['type'];
                    return null;
            }
        } catch (Exception $ex) {
            $error = $ex->getMessage();
            return null;
        }
    }

    /**
     * Get supported PDO driver
     *
     * @return null
     */
    public static function getPDODriver() {
        if (!class_exists('PDO')) {
            return null;
        }
        foreach(PDO::getAvailableDrivers() as $driver) {
            if(in_array($driver, array('sqlsrv', 'dblib'))) {
                return $driver;
            }
        }
        return null;
    }

    /**
     * Helper function to quote a string value
     *
     * @param mixed $conn Connection object/resource
     * @param string $value Value to quote
     * @return string Quoted string
     */
    protected function quote($conn, $value)
    {
        if ($conn instanceof PDO) {
            return $conn->quote($value);
        } elseif (is_resource($conn)) {
            $value = str_replace("'", "''", $value);
            $value = str_replace("\0", "[NULL]", $value);
            return "N'$value'";
        } else {
            user_error('Invalid database connection', E_USER_ERROR);
        }
        return null;
    }

    /**
     * Helper function to execute a query
     *
     * @param mixed $conn Connection object/resource
     * @param string $sql SQL string to execute
     * @return array List of first value from each resulting row
     */
    protected function query($conn, $sql)
    {
        $items = array();
        if ($conn instanceof PDO) {
            $result = $conn->query($sql);
            if ($result) {
                foreach ($result as $row) {
                    $items[] = $row[0];
                }
            }
        } elseif (is_resource($conn)) {
            $result = sqlsrv_query($conn, $sql);
            if ($result) {
                while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_NUMERIC)) {
                    $items[] = $row[0];
                }
            }
        }
        return $items;
    }

    public function requireDatabaseFunctions($databaseConfig)
    {
        $data = DatabaseAdapterRegistry::get_adapter($databaseConfig['type']);
        return !empty($data['supported']);
    }

    public function requireDatabaseServer($databaseConfig)
    {
        $conn = $this->createConnection($databaseConfig, $error);
        $success = !empty($conn);

        return array(
            'success' => $success,
            'error' => $error
        );
    }

    public function requireDatabaseConnection($databaseConfig)
    {
        $conn = $this->createConnection($databaseConfig, $error);
        $success = !empty($conn);

        return array(
            'success' => $success,
            'connection' => $conn,
            'error' => $error
        );
    }

    public function getDatabaseVersion($databaseConfig)
    {
        $conn = $this->createConnection($databaseConfig, $error);
        $result = $this->query($conn, "SELECT CONVERT(char(15), SERVERPROPERTY('ProductVersion'))");
        return empty($result) ? 0 : reset($result);
    }

    /**
     * Ensure that the SQL Server version is at least 10.00.2531 (SQL Server 2008 SP1).
     *
     * @see http://www.sqlteam.com/article/sql-server-versions
     * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
     * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
     */
    public function requireDatabaseVersion($databaseConfig)
    {
        $success = false;
        $error = '';
        $version = $this->getDatabaseVersion($databaseConfig);

        if ($version) {
            $success = version_compare($version, '10.00.2531', '>=');
            if (!$success) {
                $error = "Your SQL Server version is $version. It's recommended you use at least 10.00.2531 (SQL Server 2008 SP1).";
            }
        } else {
            $error = "Your SQL Server version could not be determined.";
        }

        return array(
            'success' => $success,
            'error' => $error
        );
    }

    public function requireDatabaseOrCreatePermissions($databaseConfig)
    {
        $conn = $this->createConnection($databaseConfig, $error);
        /** @skipUpgrade */
        if (empty($conn)) {
            $success = false;
            $alreadyExists = false;
        } elseif ($databaseConfig['type'] == 'MSSQLAzureDatabase') {
            // Don't bother with DB selection for azure, as it's not supported
            $success = true;
            $alreadyExists = true;
        } else {
            // does this database exist already?
            $list = $this->query($conn, 'SELECT NAME FROM sys.sysdatabases');
            if (in_array($databaseConfig['database'], $list)) {
                $success = true;
                $alreadyExists = true;
            } else {
                $permissions = $this->query($conn, "select COUNT(*) from sys.fn_my_permissions('','') where permission_name like 'CREATE ANY DATABASE' or permission_name like 'CREATE DATABASE';");
                $success = $permissions[0] > 0;
                $alreadyExists = false;
            }
        }

        return array(
            'success' => $success,
            'alreadyExists' => $alreadyExists
        );
    }

    public function requireDatabaseAlterPermissions($databaseConfig)
    {
        $success = false;
        $conn = $this->createConnection($databaseConfig, $error);
        if (!empty($conn)) {
            if (!$this->isAzure($databaseConfig)) {
                // Make sure to select the current database when checking permission against this database
                $this->query($conn, "USE \"{$databaseConfig['database']}\"");
            }
            $permissions = $this->query($conn, "select COUNT(*) from sys.fn_my_permissions(NULL,'DATABASE') WHERE permission_name like 'create table';");
            $success = $permissions[0] > 0;
        }

        return array(
            'success' => $success,
            'applies' => true
        );
    }
}
