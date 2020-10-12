# SQL Server Database Module

Allows SilverStripe to use SQL Server databases including SQL databases on Azure.

[![Build status](https://ci.appveyor.com/api/projects/status/hep0l5kbhu64n7l3/branch/master?svg=true)](https://ci.appveyor.com/project/silverstripe/silverstripe-mssql/branch/master)
[![Version](http://img.shields.io/packagist/v/silverstripe/silverstripe-mssql.svg?style=flat-square)](https://packagist.org/packages/silverstripe/silverstripe-mssql)
[![License](http://img.shields.io/packagist/l/silverstripe/silverstripe-mssql.svg?style=flat-square)](LICENSE)

## Requirements

 * SilverStripe 4+
 * SQL Server 2008, 2008 R2, or 2012.

`mssql` PHP api is no longer supported as of 2.0.

### *nix

Linux support is only available via the PDO extension. This requires:

 * [dblib](http://www.php.net/manual/en/ref.pdo-dblib.php)
 * [FreeTDS](http://freetds.org)

### Windows

On windows you can either connect via PDO or `sqlsrv`. Both options require the
[SQL Server Driver for PHP](https://msdn.microsoft.com/library/dn865013.aspx?f=255&MSPPError=-2147217396). "sqlsrv" 3.0+

Note: [SQL Server Express](http://www.microsoft.com/express/Database/) can also be used which is provided free by
Microsoft. However, it has limitations such as 10GB maximum database storage.

## Installation

```
composer require silverstripe/mssql ^2
```

The following environment variables will need to be set:

```
SS_DATABASE_CLASS="MSSQLAzureDatabase" # or: `MSSQLDatabase` or `MSSQLPDODatabase`
SS_DATABASE_SERVER=""
SS_DATABASE_NAME=""
SS_DATABASE_USERNAME=""
SS_DATABASE_PASSWORD=""
```

Note on OSX / Linux machines you will need to install the ODBC drivers and the PHP extension `sqlsrv` or `pdo_sqlsrv`

* https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos
* https://docs.microsoft.com/en-us/sql/connect/php/installation-tutorial-linux-mac

## Troubleshooting

*Q: SQL Server resides on a remote host (a different machine) and I can't connect to it from mine.*

A: Please ensure you have enabled TCP access using **SQL Server Configuration Manager** and [opened firewall ports](http://msdn.microsoft.com/en-us/library/ms175043.aspx).

*Q: I just installed SQL Server, but it says that it cannot connect*

A: Sometimes SQL Server will be installed as a non-default instance name, e.g. "SQLExpress" instead of "MSSQLSERVER"
(the default.) If this is the case, you'll need to declare the instance name when setting the server in your PHP
database configuration. For example: **(local)\SQLExpress**. The first part before the slash indicates the server host,
or IP address. In this case, (local) indicates localhost, which is the same server PHP is running on. The second part
is the SQL Server instance name to connect to.

*Q: I'm getting unicode SQL Server errors connecting to SQL Server database (e.g. Unicode data in a Unicode-only
collation or ntext data cannot be sent to clients using DB-Library (such as ISQL) or ODBC version 3.7 or earlier)*

A: If you are using FreeTDS make sure you're using TDS version 8.0 in **freetds.conf**. If on Windows, ensure you use
the [SQL Server Driver for PHP](http://www.microsoft.com/downloads/en/details.aspx?displaylang=en&FamilyID=ccdf728b-1ea0-48a8-a84a-5052214caad9)
and **NOT** the mssql drivers provided by PHP.

*Q: Using FreeTDS I can't connect to my SQL Server database. An error in PHP says the server doesn't exist*

A: Make sure you've got an entry in **/etc/freetds/freetds.conf** that points to your server. For example:

	[myserver]
		host = myserver.mydomain.com
		port = 1433
		tds version = 8.0

Then you can use "myserver" (the bit in square brackets above) as the server name when connecting to the database.
Note that if you're running Macports, the file is located in **/opt/local/etc/freetds/freetds.conf**.

Alternatively, if you don't want to keep adding more entries to the freetds.conf to nominate more SQL Server locations,
you can instead use the full the host/ip and port combination, such as "myserver:1433" (1433 being the default SQL
Server port.) and ensure the "tds version = 8.0" is set globally in the freetds.conf file.

**Note**: Use *tabs* not spaces when editing freetds.conf, otherwise it will not load the configuration you have
 specified!

**Note**: Certain distributions of Linux use [SELinux](http://fedoraproject.org/wiki/SELinux) which could block access
to your SQL Server database. A rule may need to be added to allow this traffic through.

