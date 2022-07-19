import os
import csv
from dotenv import load_dotenv
from datetime import datetime
from motor.motor_tornado import MotorClient
from nanoid import generate
from asyncio import get_event_loop, run

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", None)
assert MONGODB_URI is not None, "MONGODB_URI is not set"

client = MotorClient(MONGODB_URI)
db = client.get_database("busse")


BASE_FILE_PATH = os.path.join("C://", "temp")

async def read_csv_to_mongodb():
    filename = "sales.for.period.csv"
    file_path = os.path.join(BASE_FILE_PATH, filename)

    assert os.path.exists(file_path), "File does not exist"

    docs = []

    HEADER = ["DATE","CUST","CNAME","ITEM","INAME","SALE","COST","QTY","SHIP_TO_ADDR","SHIP_TO_ADDR_2","SHIP_TO_CITY","SHIP_TO_STATE","SHIP_TO_ZIP","SHIP_TO_COUNTRY","SHIP_TO_NAME","SO_NBR"]
    KEY = input("KEY: \n> ")

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        reader.__next__()
        for row in reader:
            temp = dict(zip(HEADER, row))
            temp["SALE"] = float(temp["SALE"].replace(",", ""))
            temp["COST"] = float(temp["COST"].replace(",", ""))
            temp["QTY"] = int(temp["QTY"].replace(",", ""))            
            
            temp["FREIGHT"] = 2*temp["QTY"]
            temp["OVERHEAD"] = 1*temp["QTY"]
            
            temp["REBATECREDIT"] = 0.00
            temp["TRADEFEE"] = 0.00
            
            if temp["SALE"] < 0 and temp["QTY"] == 0:
                temp["REBATECREDIT"] = temp["SALE"]
                temp["SALE"] = 0.00

            temp["DATE"] = datetime.strptime(temp["DATE"], "%m-%d-%y")

            temp["KEY"] = KEY
            temp["ID"] = generate()

            docs.append(temp)
    
    res = await db.sales.delete_many({"KEY": KEY})
    print("deleted", res.deleted_count)
    res = await db.sales.insert_many(docs)
    print("inserted", len(res.inserted_ids))
    

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(read_csv_to_mongodb())
    
