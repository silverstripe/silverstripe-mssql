<?php

namespace SilverStripe\MSSQL;

use SilverStripe\Core\Config\Config;
use SilverStripe\Core\ClassInfo;
use SilverStripe\ORM\ArrayList;
use SilverStripe\ORM\Connect\Database;
use SilverStripe\ORM\DataList;
use SilverStripe\ORM\DB;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\PaginatedList;
use SilverStripe\ORM\Queries\SQLSelect;

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
 */
class MSSQLDatabase extends Database
{

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
     * Set the default collation of the MSSQL nvarchar fields that we create.
     * We don't apply this to the database as a whole, so that we can use unicode collations.
     *
     * @param string $collation
     */
    public static function set_collation($collation)
    {
        Config::inst()->update('SilverStripe\\MSSQL\\MSSQLDatabase', 'collation', $collation);
    }

    /**
     * The default collation of the MSSQL nvarchar fields that we create.
     * We don't apply this to the database as a whole, so that we can use
     * unicode collations.
     *
     * @return string
     */
    public static function get_collation()
    {
        return Config::inst()->get('SilverStripe\\MSSQL\\MSSQLDatabase', 'collation');
    }

    /**
     * Connect to a MS SQL database.
     * @param array $parameters An map of parameters, which should include:
     *  - server: The server, eg, localhost
     *  - username: The username to log on with
     *  - password: The password to log on with
     *  - database: The database to connect to
     *  - windowsauthentication: Set to true to use windows authentication
     *    instead of username/password
     */
    public function connect($parameters)
    {
        parent::connect($parameters);

        // Configure the connection
        $this->query('SET QUOTED_IDENTIFIER ON');
        $this->query('SET TEXTSIZE 2147483647');
    }

    /**
     * Checks whether the current SQL Server version has full-text
     * support installed and full-text is enabled for this database.
     *
     * @return boolean
     */
    public function fullTextEnabled()
    {
        if ($this->fullTextEnabled === null) {
            $this->fullTextEnabled = $this->updateFullTextEnabled();
        }
        return $this->fullTextEnabled;
    }

    /**
     * Checks whether the current SQL Server version has full-text
     * support installed and full-text is enabled for this database.
     *
     * @return boolean
     */
    protected function updateFullTextEnabled()
    {
        // Check if installed
        $isInstalled = $this->query("SELECT fulltextserviceproperty('isfulltextinstalled')")->value();
        if (!$isInstalled) {
            return false;
        }

        // Check if current database is enabled
        $database = $this->getSelectedDatabase();
        $enabledForDb = $this->preparedQuery(
            "SELECT is_fulltext_enabled FROM sys.databases WHERE name = ?",
            array($database)
        )->value();
        return $enabledForDb;
    }

    public function supportsCollations()
    {
        return true;
    }

    public function supportsTimezoneOverride()
    {
        return true;
    }

    public function getDatabaseServer()
    {
        return "sqlsrv";
    }

    public function selectDatabase($name, $create = false, $errorLevel = E_USER_ERROR)
    {
        $this->fullTextEnabled = null;

        return parent::selectDatabase($name, $create, $errorLevel);
    }

    public function clearTable($table)
    {
        $this->query("TRUNCATE TABLE \"$table\"");
    }

    /**
     * SQL Server uses CURRENT_TIMESTAMP for the current date/time.
     */
    public function now()
    {
        return 'CURRENT_TIMESTAMP';
    }

    /**
     * Returns the database-specific version of the random() function
     */
    public function random()
    {
        return 'RAND()';
    }

    /**
     * The core search engine configuration.
     * Picks up the fulltext-indexed tables from the database and executes search on all of them.
     * Results are obtained as ID-ClassName pairs which is later used to reconstruct the DataObjectSet.
     *
     * @param array $classesToSearch computes all descendants and includes them. Check is done via WHERE clause.
     * @param string $keywords Keywords as a space separated string
     * @param int $start
     * @param int $pageLength
     * @param string $sortBy
     * @param string $extraFilter
     * @param bool $booleanSearch
     * @param string $alternativeFileFilter
     * @param bool $invertedMatch
     * @return PaginatedList DataObjectSet of result pages
     */
    public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "Relevance DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false)
    {
        $start = (int)$start;
        $pageLength = (int)$pageLength;
        $results = new ArrayList();

        if (!$this->fullTextEnabled()) {
            return new PaginatedList($results);
        }
        if (!in_array(substr($sortBy, 0, 9), array('"Relevanc', 'Relevance'))) {
            user_error("Non-relevance sort not supported.", E_USER_ERROR);
        }

        $allClassesToSearch = array();
        foreach ($classesToSearch as $class) {
            $allClassesToSearch = array_merge($allClassesToSearch, array_values(ClassInfo::dataClassesFor($class)));
        }
        $allClassesToSearch = array_unique($allClassesToSearch);

        //Get a list of all the tables and columns we'll be searching on:
        $fulltextColumns = $this->query('EXEC sp_help_fulltext_columns');
        $queries = array();

        // Sort the columns back into tables.
        $tables = array();
        foreach ($fulltextColumns as $column) {
            // Skip extension tables.
            if (substr($column['TABLE_NAME'], -5) == '_Live' || substr($column['TABLE_NAME'], -9) == '_versions') {
                continue;
            }

            // Add the column to table.
            $table = &$tables[$column['TABLE_NAME']];
            if (!$table) {
                $table = array($column['FULLTEXT_COLUMN_NAME']);
            } else {
                array_push($table, $column['FULLTEXT_COLUMN_NAME']);
            }
        }

        // Create one query per each table, $columns not used. We want just the ID and the ClassName of the object from this query.
        foreach ($tables as $tableName => $columns) {
            $class = DataObject::getSchema()->tableClass($tableName);
            $join = $this->fullTextSearchMSSQL($tableName, $keywords);
            if (!$join) {
                return new PaginatedList($results);
            } // avoid "Null or empty full-text predicate"

            // Check if we need to add ShowInSearch
            $where = null;
            if ($class === 'SilverStripe\\CMS\\Model\\SiteTree') {
                $where = array("\"$tableName\".\"ShowInSearch\"!=0");
            } elseif ($class === 'SilverStripe\\Assets\\File') {
                // File.ShowInSearch was added later, keep the database driver backwards compatible
                // by checking for its existence first
                $fields = $this->getSchemaManager()->fieldList($tableName);
                if (array_key_exists('ShowInSearch', $fields)) {
                    $where = array("\"$tableName\".\"ShowInSearch\"!=0");
                }
            }

            $queries[$tableName] = DataList::create($class)->where($where)->dataQuery()->query();
            $queries[$tableName]->setOrderBy(array());

            // Join with CONTAINSTABLE, a full text searcher that includes relevance factor
            $queries[$tableName]->setFrom(array("\"$tableName\" INNER JOIN $join AS \"ft\" ON \"$tableName\".\"ID\"=\"ft\".\"KEY\""));
            // Join with the base class if needed, as we want to test agains the ClassName
            if ($tableName != $tableName) {
                $queries[$tableName]->setFrom("INNER JOIN \"$tableName\" ON  \"$tableName\".\"ID\"=\"$tableName\".\"ID\"");
            }

            $queries[$tableName]->setSelect(array("\"$tableName\".\"ID\""));
            $queries[$tableName]->selectField("'$tableName'", 'Source');
            $queries[$tableName]->selectField('Rank', 'Relevance');
            if ($extraFilter) {
                $queries[$tableName]->addWhere($extraFilter);
            }
            if (count($allClassesToSearch)) {
                $classesPlaceholder = DB::placeholders($allClassesToSearch);
                $queries[$tableName]->addWhere(array(
                    "\"$tableName\".\"ClassName\" IN ($classesPlaceholder)" =>
                    $allClassesToSearch
                ));
            }
            // Reset the parameters that would get in the way
        }

        // Generate SQL
        $querySQLs = array();
        $queryParameters = array();
        foreach ($queries as $query) {
            /** @var SQLSelect $query */
            $querySQLs[] = $query->sql($parameters);
            $queryParameters = array_merge($queryParameters, $parameters);
        }

        // Unite the SQL
        $fullQuery = implode(" UNION ", $querySQLs) . " ORDER BY $sortBy";

        // Perform the search
        $result = $this->preparedQuery($fullQuery, $queryParameters);

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

        if (isset($objects)) {
            $results = new ArrayList($objects);
        } else {
            $results = new ArrayList();
        }
        $list = new PaginatedList($results);
        $list->setPageStart($start);
        $list->setPageLength($pageLength);
        $list->setTotalItems($current+1);
        return $list;
    }

    /**
     * Allow auto-increment primary key editing on the given table.
     * Some databases need to enable this specially.
     *
     * @param string $table The name of the table to have PK editing allowed on
     * @param bool $allow True to start, false to finish
     */
    public function allowPrimaryKeyEditing($table, $allow = true)
    {
        $this->query("SET IDENTITY_INSERT \"$table\" " . ($allow ? "ON" : "OFF"));
    }

    /**
     * Returns a SQL fragment for querying a fulltext search index
     *
     * @param string $tableName specific - table name
     * @param string $keywords The search query
     * @param array $fields The list of field names to search on, or null to include all
     * @return string Clause, or null if keyword set is empty or the string with JOIN clause to be added to SQL query
     */
    public function fullTextSearchMSSQL($tableName, $keywords, $fields = null)
    {
        // Make sure we are getting an array of fields
        if (isset($fields) && !is_array($fields)) {
            $fields = array($fields);
        }

        // Strip unfriendly characters, SQLServer "CONTAINS" predicate will crash on & and | and ignore others anyway.
        if (function_exists('mb_ereg_replace')) {
            $keywords = mb_ereg_replace('[^\w\s]', '', trim($keywords));
        } else {
            $keywords = $this->escapeString(str_replace(array('&', '|', '!', '"', '\''), '', trim($keywords)));
        }

        // Remove stopwords, concat with ANDs
        $keywordList = explode(' ', $keywords);
        $keywordList = $this->removeStopwords($keywordList);

        // remove any empty values from the array
        $keywordList = array_filter($keywordList);
        if (empty($keywordList)) {
            return null;
        }

        $keywords = implode(' AND ', $keywordList);
        if ($fields) {
            $fieldNames = '"' . implode('", "', $fields) . '"';
        } else {
            $fieldNames = "*";
        }

        return "CONTAINSTABLE(\"$tableName\", ($fieldNames), '$keywords')";
    }

    /**
     * Remove stopwords that would kill a MSSQL full-text query
     *
     * @param array $keywords
     *
     * @return array $keywords with stopwords removed
     */
    public function removeStopwords($keywords)
    {
        $goodKeywords = array();
        foreach ($keywords as $keyword) {
            if (in_array($keyword, self::$noiseWords)) {
                continue;
            }
            $goodKeywords[] = trim($keyword);
        }
        return $goodKeywords;
    }

    /**
     * Does this database support transactions?
     */
    public function supportsTransactions()
    {
        return $this->supportsTransactions;
    }

    /**
     * This is a quick lookup to discover if the database supports particular extensions
     * Currently, MSSQL supports no extensions
     *
     * @param array $extensions List of extensions to check for support of. The key of this array
     * will be an extension name, and the value the configuration for that extension. This
     * could be one of partitions, tablespaces, or clustering
     * @return boolean Flag indicating support for all of the above
     */
    public function supportsExtensions($extensions = array('partitions', 'tablespaces', 'clustering'))
    {
        if (isset($extensions['partitions'])) {
            return false;
        } elseif (isset($extensions['tablespaces'])) {
            return false;
        } elseif (isset($extensions['clustering'])) {
            return false;
        } else {
            return false;
        }
    }

    /**
     * Start transaction. READ ONLY not supported.
     *
     * @param bool $transactionMode
     * @param bool $sessionCharacteristics
     */
    public function transactionStart($transactionMode = false, $sessionCharacteristics = false)
    {
        if ($this->connector instanceof SQLServerConnector) {
            $this->connector->transactionStart();
        } else {
            $this->query('BEGIN TRANSACTION');
        }
    }

    public function transactionSavepoint($savepoint)
    {
        $this->query("SAVE TRANSACTION \"$savepoint\"");
    }

    public function transactionRollback($savepoint = false)
    {
        if ($savepoint) {
            $this->query("ROLLBACK TRANSACTION \"$savepoint\"");
        } elseif ($this->connector instanceof SQLServerConnector) {
            $this->connector->transactionRollback();
        } else {
            $this->query('ROLLBACK TRANSACTION');
        }
    }

    public function transactionEnd($chain = false)
    {
        if ($this->connector instanceof SQLServerConnector) {
            $this->connector->transactionEnd();
        } else {
            $this->query('COMMIT TRANSACTION');
        }
    }

    public function comparisonClause($field, $value, $exact = false, $negate = false, $caseSensitive = null, $parameterised = false)
    {
        if ($exact) {
            $comp = ($negate) ? '!=' : '=';
        } else {
            $comp = 'LIKE';
            if ($negate) {
                $comp = 'NOT ' . $comp;
            }
        }

        // Field definitions are case insensitive by default,
        // change used collation for case sensitive searches.
        $collateClause = '';
        if ($caseSensitive === true) {
            if (self::get_collation()) {
                $collation = preg_replace('/_CI_/', '_CS_', self::get_collation());
            } else {
                $collation = 'Latin1_General_CS_AS';
            }
            $collateClause = ' COLLATE ' . $collation;
        } elseif ($caseSensitive === false) {
            if (self::get_collation()) {
                $collation = preg_replace('/_CS_/', '_CI_', self::get_collation());
            } else {
                $collation = 'Latin1_General_CI_AS';
            }
            $collateClause = ' COLLATE ' . $collation;
        }

        $clause = sprintf("%s %s %s", $field, $comp, $parameterised ? '?' : "'$value'");
        if ($collateClause) {
            $clause .= $collateClause;
        }

        return $clause;
    }

    /**
     * Function to return an SQL datetime expression for MSSQL
     * used for querying a datetime in a certain format
     *
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
    public function formattedDatetimeClause($date, $format)
    {
        preg_match_all('/%(.)/', $format, $matches);
        foreach ($matches[1] as $match) {
            if (array_search($match, array('Y', 'm', 'd', 'H', 'i', 's', 'U')) === false) {
                user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);
            }
        }

        if (preg_match('/^now$/i', $date)) {
            $date = "CURRENT_TIMESTAMP";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
            $date = "'$date.000'";
        }

        if ($format == '%U') {
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
        while (strlen($buffer)) {
            if (substr($buffer, 0, 1) == '%') {
                $f = substr($buffer, 1, 1);
                $flen = $f == 'Y' ? 4 : 2;
                $strings[] = "RIGHT('0' + CAST(DATEPART({$trans[$f]},$date) AS VARCHAR), $flen)";
                $buffer = substr($buffer, 2);
            } else {
                $pos = strpos($buffer, '%');
                if ($pos === false) {
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
     *
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
    public function datetimeIntervalClause($date, $interval)
    {
        $trans = array(
            'year' => 'yy',
            'month' => 'mm',
            'day' => 'dd',
            'hour' => 'hh',
            'minute' => 'mi',
            'second' => 'ss',
        );

        $singularinterval = preg_replace('/(year|month|day|hour|minute|second)s/i', '$1', $interval);

        if (
            !($params = preg_match('/([-+]\d+) (\w+)/i', $singularinterval, $matches)) ||
            !isset($trans[strtolower($matches[2])])
        ) {
            user_error('datetimeIntervalClause(): invalid interval ' . $interval, E_USER_WARNING);
        }

        if (preg_match('/^now$/i', $date)) {
            $date = "CURRENT_TIMESTAMP";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
            $date = "'$date'";
        }

        return "CONVERT(VARCHAR, DATEADD(" . $trans[strtolower($matches[2])] . ", " . (int)$matches[1] . ", $date), 120)";
    }

    /**
     * Function to return an SQL datetime expression for MSSQL.
     * used for querying a datetime substraction
     *
     * @param string $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
     * @param string $date2 to be substracted of $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
     * @return string SQL datetime expression to query for the interval between $date1 and $date2 in seconds which is the result of the substraction
     */
    public function datetimeDifferenceClause($date1, $date2)
    {
        if (preg_match('/^now$/i', $date1)) {
            $date1 = "CURRENT_TIMESTAMP";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
            $date1 = "'$date1'";
        }

        if (preg_match('/^now$/i', $date2)) {
            $date2 = "CURRENT_TIMESTAMP";
        } elseif (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
            $date2 = "'$date2'";
        }

        return "DATEDIFF(s, $date2, $date1)";
    }
}
