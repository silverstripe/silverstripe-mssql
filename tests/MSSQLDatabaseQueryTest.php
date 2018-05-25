<?php

use SilverStripe\ORM\DataObject;
use SilverStripe\Dev\SapphireTest;
use SilverStripe\Dev\TestOnly;

class MSSQLDatabaseQueryTest extends SapphireTest
{
    protected static $fixture_file = 'MSSQLDatabaseQueryTest.yml';

    public static function getExtraDataObjects()
    {
        return ['MSSQLDatabaseQueryTestDataObject'];
    }

    public function testDateValueFormatting()
    {
        $obj = $this->objFromFixture('MSSQLDatabaseQueryTestDataObject', 'test-data-1');
        $this->assertEquals('2012-01-01', $obj->obj('TestDate')->Format('y-MM-dd'), 'Date field value is formatted correctly (y-MM-dd)');
    }

    public function testDatetimeValueFormatting()
    {
        $obj = $this->objFromFixture('MSSQLDatabaseQueryTestDataObject', 'test-data-1');
        $this->assertEquals('2012-01-01 10:30:00', $obj->obj('TestDatetime')->Format('y-MM-dd HH:mm:ss'), 'Datetime field value is formatted correctly (y-MM-dd HH:mm:ss)');
    }
}

class MSSQLDatabaseQueryTestDataObject extends DataObject implements TestOnly
{
    private static $db = array(
        'TestDate' => 'Date',
        'TestDatetime' => 'Datetime'
    );
}
