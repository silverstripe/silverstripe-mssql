<?php

namespace SilverStripe\MSSQL;


use SilverStripe\ORM\Connect\SS_Query;
use DateTime;


/**
 * A result-set from a MSSQL database.
 * 
 * @package mssql
 */
class SQLServerQuery extends SS_Query
{

    /**
     * The SQLServerConnector object that created this result set.
     * 
     * @var SQLServerConnector
     */
    private $connector;

    /**
     * The internal MSSQL handle that points to the result set.
     * 
     * @var resource
     */
    private $handle;

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

    public function seek($row)
    {
        if (!is_resource($this->handle)) {
            return false;
        }

        user_error('MSSQLQuery::seek() not supported in sqlsrv', E_USER_WARNING);
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
        if (!is_resource($this->handle)) {
            return false;
        }

        if ($data = sqlsrv_fetch_array($this->handle, SQLSRV_FETCH_ASSOC)) {
            // special case for sqlsrv - date values are DateTime coming out of the sqlsrv drivers,
            // so we convert to the usual Y-m-d H:i:s value!
            foreach ($data as $name => $value) {
                if ($value instanceof DateTime) {
                    $data[$name] = $value->format('Y-m-d H:i:s');
                }
            }
            return $data;
        } else {
            // Free the handle if there are no more results - sqlsrv crashes if there are too many handles
            sqlsrv_free_stmt($this->handle);
            $this->handle = null;
        }

        return false;
    }
}
