<?php

use SilverStripe\ORM\DataObject;
use SilverStripe\Dev\SapphireTest;
use SilverStripe\Dev\TestOnly;

class MSSQLDatabaseQueryTest extends SapphireTest
{

    public static $fixture_file = 'MSSQLDatabaseQueryTest.yml';

    protected $extraDataObjects = array(
        'MSSQLDatabaseQueryTestDataObject'
    );

    public function testDateValueFormatting()
    {
        $obj = $this->objFromFixture('MSSQLDatabaseQueryTestDataObject', 'test-data-1');
        $this->assertEquals('2012-01-01', $obj->obj('TestDate')->Format('Y-m-d'), 'Date field value is formatted correctly (Y-m-d)');
    }

    public function testDatetimeValueFormatting()
    {
        $obj = $this->objFromFixture('MSSQLDatabaseQueryTestDataObject', 'test-data-1');
        $this->assertEquals('2012-01-01 10:30:00', $obj->obj('TestDatetime')->Format('Y-m-d H:i:s'), 'Datetime field value is formatted correctly (Y-m-d H:i:s)');
    }
}
class MSSQLDatabaseQueryTestDataObject extends DataObject implements TestOnly
{

    private static $db = array(
        'TestDate' => 'Date',
        'TestDatetime' => 'Datetime'
    );
}
