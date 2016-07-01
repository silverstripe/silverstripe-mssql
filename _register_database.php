<?php

// PDO connector for MS SQL Server
/** @skipUpgrade */
DatabaseAdapterRegistry::register(array(
	'class' => 'MSSQLPDODatabase',
	'title' => 'SQL Server 2008 (using PDO)',
	'helperPath' => dirname(__FILE__).'/code/MSSQLDatabaseConfigurationHelper.php',
	'supported' => (class_exists('PDO') && in_array('sqlsrv', PDO::getAvailableDrivers())),
	'missingExtensionText' =>
		'Either the <a href="http://www.php.net/manual/en/book.pdo.php">PDO Extension</a> or 
		the <a href="http://www.php.net/manual/en/ref.pdo-sqlsrv.php">SQL Server PDO Driver</a> 
		are unavailable. Please install or enable these and refresh this page.'
));

// Basic driver using sqlsrv connector
/** @skipUpgrade */
DatabaseAdapterRegistry::register(array(
	'class' => 'MSSQLDatabase',
	'title' => 'SQL Server 2008 (using sqlsrv)',
	'helperPath' => dirname(__FILE__).'/code/MSSQLDatabaseConfigurationHelper.php',
	'supported' => function_exists('sqlsrv_connect'),
	'missingExtensionText' =>
		'The <a href="http://www.microsoft.com/sqlserver/2005/en/us/PHP-Driver.aspx">sqlsrv</a>
		PHP extensions is not available. Please install or enable it and refresh this page.',
	'fields' => array_merge(DatabaseAdapterRegistry::get_default_fields(), array(
		// @todo - do we care about windows authentication for PDO/SQL Server?
		'windowsauthentication' => array(
			'title' => 'Use Windows authentication? (leave blank for false)',
			'default' => ''
		)
	))
));

// MS Azure uses an online database
/** @skipUpgrade */
DatabaseAdapterRegistry::register(array(
	'class' => 'MSSQLAzureDatabase',
	'title' => 'MS Azure Database (using sqlsrv)',
	'helperPath' => dirname(__FILE__).'/code/MSSQLDatabaseConfigurationHelper.php',
	'supported' => function_exists('sqlsrv_connect'),
	'missingExtensionText' =>
		'The <a href="http://www.microsoft.com/sqlserver/2005/en/us/PHP-Driver.aspx">sqlsrv</a>
		PHP extension is not available. Please install or enable it and refresh this page.'
));
