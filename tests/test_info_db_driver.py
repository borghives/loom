from loom.info.db_driver import MongoDbModelDriver

def test_mongodb_model_driver_collection_name():
    # Test 1: Standard usage
    driver = MongoDbModelDriver(collection_name="users", db_name="test_db", client_factory=None)
    assert driver.get_db_collection_name() == "users"

    # Test 2: With version
    driver = MongoDbModelDriver(collection_name="users", db_name="test_db", client_factory=None, version=2)
    assert driver.get_db_collection_name() == "users_v2"

    # Test 3: With test flag
    driver = MongoDbModelDriver(collection_name="users", db_name="test_db", client_factory=None, test=True)
    assert driver.get_db_collection_name() == "users_test"

    # Test 4: With version and test flag
    driver = MongoDbModelDriver(collection_name="users", db_name="test_db", client_factory=None, version=3, test=True)
    assert driver.get_db_collection_name() == "users_v3_test"
