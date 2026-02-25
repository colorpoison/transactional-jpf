package benchmarks.tpcc.procedures;

import benchmarks.tpcc.TPCC;
import benchmarks.tpcc.TPCCUtility;
import benchmarks.tpcc.objects.Customer;
import benchmarks.tpcc.objects.District;
import benchmarks.tpcc.objects.History;
import benchmarks.tpcc.objects.Warehouse;
import database.AbortDatabaseException;
import database.APIDatabase;

import java.util.ArrayList;
import java.util.HashMap;

public class Payment extends BasicTPCCProcedure{

    public Payment(APIDatabase db){
        super(db);
    }

    public void payment(int warehouseID, int districtID, int customerID,
                               String customerName, boolean customerIDSearch, float paymentAmount){

        try {
            db.begin("Payment");

            updateWarehouse(warehouseID, paymentAmount);


            var w = getWarehouse(warehouseID);
            updateDistrictPayment(warehouseID, districtID, paymentAmount);

            var d = getDistrict(warehouseID, districtID);

            var c = getCustomerAndPay(
                    warehouseID, districtID, customerID, customerName, customerIDSearch, paymentAmount);

            if(c.getCredit().equals("BC")) {

                var cData = getData(warehouseID, districtID, c, paymentAmount);
                if (cData != null) {
                    c.setData(cData);
                }

                //BC
                updateBalanceData(c);
            }
            else {
                //not BC (that is not equal to !bc)
                updateBalanceData(c);
            }
            insertHistory(w, d, c, paymentAmount);
            db.commit();
        } catch (AbortDatabaseException ignored) {
        }

    }

    private Customer getCustomerAndPay(int warehouseID, int districtID,
                                              int customerID, String customerName,
                                              boolean customerIDSearch, float payment) throws AbortDatabaseException {
        var c = getCustomer(warehouseID, districtID, customerID, customerName, customerIDSearch);

        if(c == null) db.abort();
        c.setBalance(c.getBalance() - payment);
        c.setYtdPayment(c.getYtdPayment() + payment);
        c.setPaymentCnt(c.getPaymentCnt()+1);
        return c;
    }

    private String getData( int warehouseID, int districtID, Customer c, float paymentAmount) throws AbortDatabaseException {

        if(c == null) db.abort();
        var cus = getCustomerById(warehouseID, districtID, c.getID());

        if(cus == null) db.abort();

        var cData = c.getID() + " " + c.getDistrictID() + " " + c.getWarehouseID() + " " +
                districtID + " " + warehouseID + " " + paymentAmount + " | " + cus.getData();
        if (cData.length() > 500) {
            cData = cData.substring(0, 500);
        }
        return cData;


    }

    private void updateBalanceData(Customer c){

        if(c != null) {
            db.writeRow(TPCC.CUSTOMER, c.getKey(), c.toString());
        }

    }

    private void insertHistory(Warehouse w, District d,
                                      Customer c, float paymentAmount)  {


        var wName = w.getName();
        var dName = d.getName();
        if (wName.length() > 10) {
            wName = wName.substring(0, 10);
        }
        if (dName.length() > 10) {
            dName = dName.substring(0, 10);
        }
        String hData = wName + " " + dName;

        var h = new History(c.getID(), c.getDistrictID(), c.getWarehouseID(),d.getID(),
                w.getID(),System.currentTimeMillis(), paymentAmount, hData);

        db.writeRow(TPCC.HISTORY, h.getKey(), h.toString());
    }

    private void updateWarehouse(int warehouseID, float paymentAmount){

        var wSt = db.readRow(TPCC.WAREHOUSE, warehouseID+"");
        var w = wSt == null ? null : new Warehouse(wSt);

        if(w == null) return;
        w.setYtd(paymentAmount);
        db.writeRow(TPCC.WAREHOUSE, warehouseID+"", w.toString());
    }

    private void updateDistrictPayment(int warehouseID, int districtID, float paymentAmount){

        var dSt = db.readRow(TPCC.DISTRICT, warehouseID+":"+districtID);
        var d = dSt == null ? null : new District(dSt);

        if(d != null) {
            d.setYtd(paymentAmount);
            db.writeRow(TPCC.DISTRICT, warehouseID+":"+districtID, d.toString());
        }

    }



}
