<?php

use SilverStripe\Dev\Install\DatabaseAdapterRegistry;
use SilverStripe\MSSQL\MSSQLDatabaseConfigurationHelper;

// PDO connector for MS SQL Server
/** @skipUpgrade */
DatabaseAdapterRegistry::register(array(
	'class' => 'MSSQLPDODatabase',
    'module' => 'mssql',
	'title' => 'SQL Server 2008 (using PDO)',
	'helperPath' => __DIR__.'/src/MSSQLDatabaseConfigurationHelper.php',
    'helperClass' => MSSQLDatabaseConfigurationHelper::class,
	'supported' => !!MSSQLDatabaseConfigurationHelper::getPDODriver(),
	'missingExtensionText' =>
		'Either the <a href="http://www.php.net/manual/en/book.pdo.php">PDO Extension</a> or
		the <a href="http://www.php.net/manual/en/ref.pdo-sqlsrv.php">SQL Server PDO Driver</a>
		are unavailable. Please install or enable these and refresh this page.'
));

// Basic driver using sqlsrv connector
/** @skipUpgrade */
DatabaseAdapterRegistry::register(array(
	'class' => 'MSSQLDatabase',
    'module' => 'mssql',
	'title' => 'SQL Server 2008 (using sqlsrv)',
	'helperPath' => __DIR__.'/src/MSSQLDatabaseConfigurationHelper.php',
    'helperClass' => MSSQLDatabaseConfigurationHelper::class,
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
    'module' => 'mssql',
	'title' => 'MS Azure Database (using sqlsrv)',
	'helperPath' => __DIR__.'/src/MSSQLDatabaseConfigurationHelper.php',
    'helperClass' => MSSQLDatabaseConfigurationHelper::class,
	'supported' => function_exists('sqlsrv_connect'),
	'missingExtensionText' =>
		'The <a href="http://www.microsoft.com/sqlserver/2005/en/us/PHP-Driver.aspx">sqlsrv</a>
		PHP extension is not available. Please install or enable it and refresh this page.'
));
