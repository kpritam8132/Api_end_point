from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from clickhouse_connect import get_client
from datetime import datetime
import uvicorn

app = FastAPI(title="Vehicle Service History API V2")

# -----------------------------
# 1. Connect to ClickHouse
# -----------------------------
client = get_client(
    host='localhost',   # ClickHouse host
    port=8123,          # HTTP port
    username='admin',
    password='changeme'
)

# -----------------------------
# 2. Create database & table
# -----------------------------
client.command("CREATE DATABASE IF NOT EXISTS vehicle_db")

client.command("""
DROP TABLE IF EXISTS vehicle_db.service_history
""")

client.command("""
CREATE TABLE IF NOT EXISTS vehicle_db.service_history
(
    vehicleNumber String,
    labourAmount Float64,
    partAmount Float64,
    totalAmount Float64,
    dateOfBill String,
    repairOrderDate String,
    dealerAddress String,
    groupOfParent String,
    srVehicleCd String,
    cdLoc String,
    nameOfSA String,
    noOfJobCard String,
    dateOfSVC String,
    noOfRO String,
    dealerName String,
    dealerNo UInt32,
    mileage UInt32,
    serviceType String,
    typOfPayment Nullable(String)
)
ENGINE = MergeTree
ORDER BY vehicleNumber
""")

# -----------------------------
# 3. Define JSON schema
# -----------------------------
class ServiceDetail(BaseModel):
    labourAmount: float
    partAmount: float
    totalAmount: float
    dateOfBill: str
    repairOrderDate: str
    dealerAddress: str
    groupOfParent: str
    srVehicleCd: str
    cdLoc: str
    nameOfSA: str
    noOfJobCard: str
    dateOfSVC: str
    noOfRO: str
    dealerName: str
    dealerNo: int
    mileage: int
    serviceType: str
    typOfPayment: Optional[str] = None


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
            # Convert all dates to YYYY-MM-DD string
            def convert_date(d):
                try:
                    return datetime.strptime(d, "%d/%m/%Y").strftime("%Y-%m-%d")
                except:
                    return d  # fallback if format is unexpected

            date_of_bill = convert_date(svc.dateOfBill)
            repair_order_date = convert_date(svc.repairOrderDate)
            date_of_svc = convert_date(svc.dateOfSVC)

            insert_data.append([
                vehicle_number,
                svc.labourAmount,
                svc.partAmount,
                svc.totalAmount,
                date_of_bill,
                repair_order_date,
                svc.dealerAddress,
                svc.groupOfParent,
                svc.srVehicleCd,
                svc.cdLoc,
                svc.nameOfSA,
                svc.noOfJobCard,
                date_of_svc,
                svc.noOfRO,
                svc.dealerName,
                svc.dealerNo,
                svc.mileage,
                svc.serviceType,
                svc.typOfPayment
            ])

        # Insert all records into ClickHouse
        client.insert(
            'vehicle_db.service_history',
            insert_data,
            column_names=[
                'vehicleNumber',
                'labourAmount',
                'partAmount',
                'totalAmount',
                'dateOfBill',
                'repairOrderDate',
                'dealerAddress',
                'groupOfParent',
                'srVehicleCd',
                'cdLoc',
                'nameOfSA',
                'noOfJobCard',
                'dateOfSVC',
                'noOfRO',
                'dealerName',
                'dealerNo',
                'mileage',
                'serviceType',
                'typOfPayment'
            ]
        )

        return {"status": "success", "rows_inserted": len(insert_data)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 5. Run FastAPI server
# -----------------------------
if __name__ == "__main__":
    uvicorn.run("service_history_api_v2:app", host="0.0.0.0", port=8000, reload=True)
