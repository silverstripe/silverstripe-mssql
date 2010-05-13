<?php
/**
 * This is a helper class for the SS installer.
 * 
 * It does all the specific checking for MSSQLDatabase
 * to ensure that the configuration is setup correctly.
 * 
 * @package mssql
 */
class MSSQLDatabaseConfigurationHelper implements DatabaseConfigurationHelper {

	/**
	 * Ensure that the database function for connectivity is available.
	 * If it is, we assume the PHP module for this database has been setup correctly.
	 * 
	 * @param array $databaseConfig Associative array of database configuration, e.g. "server", "username" etc
	 * @return boolean
	 */
	public function requireDatabaseFunctions($databaseConfig) {
		return (function_exists('mssql_connect') || function_exists('sqlsrv_connect')) ? true : false;
	}

	/**
	 * Ensure that the database server exists.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseServer($databaseConfig) {
		$success = false;
		$error = '';
		
		if(function_exists('mssql_connect')) {
			$conn = @mssql_connect($databaseConfig['server'], $databaseConfig['username'], $databaseConfig['password'], true);
		} else {
			$conn = @sqlsrv_connect($databaseConfig['server'], array(
				'UID' => $databaseConfig['username'],
				'PWD' => $databaseConfig['password']
			));
			
			$errors = sqlsrv_errors();
			if($errors) {
				$error .= "\n";
				foreach($errors as $detail) {
					$error .= "\n" . @$detail['message'] . "\n";
				}
			}
		}
		
		if($conn) {
			$success = true;
		} else {
			$success = false;
			if(!$error) $error = 'SQL Server requires a valid username and password to determine if the server exists.';
		}
		
		return array(
			'success' => $success,
			'error' => $error
		);
	}

	/**
	 * Ensure a database connection is possible using credentials provided.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseConnection($databaseConfig) {
		$success = false;
		$error = '';

		if(function_exists('mssql_connect')) {
			$conn = @mssql_connect($databaseConfig['server'], $databaseConfig['username'], $databaseConfig['password'], true);
		} else {
			$conn = @sqlsrv_connect($databaseConfig['server'], array(
				'UID' => $databaseConfig['username'],
				'PWD' => $databaseConfig['password']
			));
			
			$errors = sqlsrv_errors();
			if($errors) {
				$error .= "\n";
				foreach($errors as $detail) {
					$error .= "\n" . @$detail['message'] . "\n";
				}
			}
		}
		
		if($conn) {
			$success = true;
		} else {
			$success = false;
		}
		
		return array(
			'success' => $success,
			'connection' => $conn,
			'error' => $error
		);
	}

	/**
	 * Ensure that the SQL Server version is at least 10.00.1600 (SQL Server 2008 RTM).
	 * @see http://www.sqlteam.com/article/sql-server-versions
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseVersion($databaseConfig) {
		$success = false;
		$error = '';
		$version = 0;

		// Get the version using SERVERPROPERTY() function
		if(function_exists('mssql_connect')) {
			$conn = @mssql_connect($databaseConfig['server'], $databaseConfig['username'], $databaseConfig['password'], true);
			$result = @mssql_query("SELECT SERVERPROPERTY('ProductVersion')", $conn);
			$row = @mssql_fetch_array($result);
		} else {
			$conn = @sqlsrv_connect($databaseConfig['server'], array(
				'UID' => $databaseConfig['username'],
				'PWD' => $databaseConfig['password']
			));
			$result = @sqlsrv_query($conn, "SELECT SERVERPROPERTY('ProductVersion')");
			$row = @sqlsrv_fetch_array($result);
		}

		if($row && isset($row[0])) {
			$version = $row[0];
		}

		if($version) {
			$success = version_compare($version, '10.00.1600', '>=');
			if(!$success) {
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

	/**
	 * Ensure that the database connection is able to use an existing database,
	 * or be able to create one if it doesn't exist.
	 * 
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'alreadyExists' => 'true')
	 */
	public function requireDatabaseOrCreatePermissions($databaseConfig) {
		$success = false;
		$alreadyExists = false;

		$check = $this->requireDatabaseConnection($databaseConfig);
		$conn = $check['connection'];

		if(function_exists('mssql_select_db')) {
			$exists = @mssql_select_db($databaseConfig['database'], $conn);
		} else {
			$exists = @sqlsrv_query($conn, "USE \"$databaseConfig[database]\"");
		}

		if($exists) {
			$success = true;
			$alreadyExists = true;
		} else {
			if(function_exists('mssql_query') && mssql_query("CREATE DATABASE testing123", $conn)) {
				mssql_query("DROP DATABASE testing123", $conn);
				$success = true;
				$alreadyExists = false;
			} elseif(function_exists('sqlsrv_query') && @sqlsrv_query($conn, "CREATE DATABASE testing123")) {
				sqlsrv_query($conn, "DROP DATABASE testing123");
				$success = true;
				$alreadyExists = false;
			}
		}

		return array(
			'success' => $success,
			'alreadyExists' => $alreadyExists
		);
	}

}