<?php

namespace SilverStripe\MSSQL;

use SilverStripe\ORM\Connect\DatabaseException;
use SilverStripe\ORM\Connect\Query;
use function sqlsrv_fetch_array;
use function sqlsrv_num_rows;
use function sqlsrv_free_stmt;
use const SQLSRV_SCROLL_ABSOLUTE;

/**
 * A result-set from a MSSQL database.
 */
class SQLServerQuery extends Query
{

    /**
     * The SQLServerConnector object that created this result set.
     *
     * @var SQLServerConnector
     */
    protected $connector;

    /**
     * The internal MSSQL handle that points to the result set.
     *
     * @var resource
     */
    protected $handle;

    /**
     * Hook the result-set given into a Query class, suitable for use by sapphire.
     * @param SQLServerConnector $connector The database object that created this query.
     * @param resource $handle the internal mssql handle that is points to the resultset.
     */
    public function __construct(SQLServerConnector $connector, $handle)
    {
        $this->connector = $connector;
        $this->handle = $handle;
    }

    public function __destruct()
    {
        if (is_resource($this->handle)) {
            sqlsrv_free_stmt($this->handle);
        }
    }

    public function numRecords()
    {
        if (!is_resource($this->handle)) {
            return false;
        }

        // WARNING: This will only work if the cursor type is scrollable!
        if (function_exists('sqlsrv_num_rows')) {
            return sqlsrv_num_rows($this->handle);
        } else {
            user_error('MSSQLQuery::numRecords() not supported in this version of sqlsrv', E_USER_WARNING);
        }
    }

    public function nextRecord()
    {
        if (is_resource($this->handle)) {
            $row = sqlsrv_fetch_array($this->handle, SQLSRV_FETCH_ASSOC);

            return $row;
        } else {
            return false;
        }
    }

    public function seek($row)
    {
        if (is_object($this->handle)) {
            sqlsrv_fetch($this->handle, SQLSRV_SCROLL_ABSOLUTE, $row);
            $result = $this->nextRecord();
            sqlsrv_fetch($this->handle, SQLSRV_SCROLL_ABSOLUTE, $row);
            return $result;
        }
        return null;
    }
}
