from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from clickhouse_connect import get_client
from datetime import datetime
import uvicorn

app = FastAPI(title="Vehicle Service History API")

# -----------------------------
# 1. Connect to ClickHouse
# -----------------------------
client = get_client(
    host='localhost',   # ClickHouse host
    port=8123,          # ClickHouse HTTP port
    username='admin',
    password='changeme'
)

# -----------------------------
# 2. Create database & table
# -----------------------------
client.command("CREATE DATABASE IF NOT EXISTS service_db")

client.command("""
CREATE TABLE IF NOT EXISTS service_db.service_history
(
    vehicleNumber String,
    dealerName String,
    totalAmmount String,
    dateOfSVC String,
    dealerNo String,
    serviceType String,
    noOfRo String,
    mileAge String,
    typeOfPayment String
)
ENGINE = MergeTree
ORDER BY vehicleNumber
""")

# -----------------------------
# 3. Define JSON schema
# -----------------------------
class ServiceDetail(BaseModel):
    dealerName: str
    totalAmmount: str
    dateOfSVC: str
    dealerNo: str
    serviceType: str
    noOfRo: str
    mileAge: str
    typeOfPayment: str

class Result(BaseModel):
    vehicleNumber: str
    serviceHistoryDetails: List[ServiceDetail]

class ServicePayload(BaseModel):
    code: int
    message: str
    result: Result

# -----------------------------
# 4. Endpoint for inserting data
# -----------------------------
@app.post("/insert_service_history")
def insert_service_history(payload: ServicePayload):
    try:
        vehicle_number = payload.result.vehicleNumber
        service_records = payload.result.serviceHistoryDetails

        insert_data = []

        for svc in service_records:
            # Convert dateOfSVC to string YYYY-MM-DD format
            try:
                date_str = datetime.strptime(svc.dateOfSVC, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                # fallback: keep original string if format is unexpected
                date_str = svc.dateOfSVC

            insert_data.append([
                vehicle_number,
                svc.dealerName,
                svc.totalAmmount,
                date_str,
                svc.dealerNo,
                svc.serviceType,
                svc.noOfRo,
                svc.mileAge,
                svc.typeOfPayment
            ])

        # Insert all records into ClickHouse
        client.insert(
            'service_db.service_history',
            insert_data,
            column_names=[
                'vehicleNumber',
                'dealerName',
                'totalAmmount',
                'dateOfSVC',
                'dealerNo',
                'serviceType',
                'noOfRo',
                'mileAge',
                'typeOfPayment'
            ]
        )

        return {"status": "success", "rows_inserted": len(insert_data)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------
# 5. Run FastAPI server
# -----------------------------
if __name__ == "__main__":
    uvicorn.run("service_history_api:app", host="0.0.0.0", port=8000, reload=True)
