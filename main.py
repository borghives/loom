from bson import ObjectId
import pandas as pd
import polars as pl
from pymongo import MongoClient
from pymongoarrow.api import write # type: ignore
import rich
# from loom.info.persistable import Persistable, declare_persist_db

# @declare_persist_db(db_name="test_db", collection_name="test_collection", version=1, test=True)
# class TestModel(Persistable):
#     name: str
#     value: int
#     link_id: ObjectId | None = None

# test_fld = TestModel.fields()

# def main():
#     fix_id = ObjectId("68d6ea0de95339c6e73ecd05")
#     fix_id_2 = ObjectId("68d6ea0de95339c6e73ecd06")
#     df = pd.DataFrame([
#             {"name": "df_user1", "value": 11, "link_id": fix_id},
#             {"name": "df_user2", "value": 20, "link_id": ObjectId()},
#         ])
#     TestModel.insert_dataframe(df)

#     pldf = TestModel.filter().load_polars()
#     assert isinstance(pldf, pl.DataFrame)
#     rich.print(pldf)

#     filtered = pldf.filter(
#         pl.col("link_id") == fix_id.binary
#     ).with_columns(
#         (pl.col("value") + 2).alias("new_value")
#     )

#     # TestModel.write_dataframe(filtered)

#     rich.print(filtered)

def main():
    rich.print("Hello, Loom!")
    # Create a sample Pandas DataFrame with an ObjectId column
    data = {'_id': [ObjectId(), ObjectId()],
            'link_id': [ObjectId(), ObjectId()],
            'name': ['Alice', 'Bob']}
    df = pd.DataFrame(data)
    rich.print(df)

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['test']
    collection = db['test']
    
    write(collection, df)



if __name__ == "__main__":
    main()