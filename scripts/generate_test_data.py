import random
from faker import Faker
from tests.performance_test_model import PerformanceTestModel
import pymongo

# 2. Create script to generate and insert data
def generate_data(num_records: int):
    fake = Faker()
    records = [
        PerformanceTestModel(
            name=fake.name(),
            value=random.random() * 1000,
            notes=fake.text(),
        ).dump_doc() for _ in range(num_records)
    ]
    
    # Use a direct pymongo client for bulk insert for speed
    collection = PerformanceTestModel.get_db_collection()
    # Clear existing data
    collection.delete_many({})
    collection.insert_many(records)
    print(f"Inserted {num_records} records into {collection.full_name}")

if __name__ == "__main__":
    generate_data(100_000)
