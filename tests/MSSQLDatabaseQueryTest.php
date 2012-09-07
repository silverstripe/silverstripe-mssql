<?php
class MSSQLDatabaseQueryTest extends SapphireTest {

	public static $fixture_file = 'MSSQLDatabaseQueryTest.yml';

	protected $extraDataObjects = array(
		'MSSQLDatabaseQueryTestDataObject'	
	);

	public function testDateValueFormatting() {
		$obj = $this->objFromFixture('MSSQLDatabaseQueryTestDataObject', 'test-data-1');
		$this->assertEquals('2012-01-01', date('Y-m-d', strtotime($obj->TestDate)), 'Date field value is formatted correctly (Y-m-d)');
	}

	public function testDatetimeValueFormatting() {
		$obj = $this->objFromFixture('MSSQLDatabaseQueryTestDataObject', 'test-data-1');
		$this->assertEquals('2012-01-01 10:30:00', $obj->TestDatetime, 'Datetime field value is formatted correctly (Y-m-d H:i:s)');
	}

}
class MSSQLDatabaseQueryTestDataObject extends DataObject implements TestOnly {

	public static $db = array(
		'TestDate' => 'Date',
		'TestDatetime' => 'SS_Datetime'
	);

}
