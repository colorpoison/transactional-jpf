package benchmarks.tpcc.procedures;

import benchmarks.tpcc.TPCC;
import benchmarks.tpcc.TPCCUtility;
import benchmarks.tpcc.objects.Customer;
import benchmarks.tpcc.objects.NewOrder;
import benchmarks.tpcc.objects.Order;
import benchmarks.tpcc.objects.OrderLine;
import database.APIDatabase;
import database.AbortDatabaseException;

import java.util.ArrayList;

public class Delivery extends BasicTPCCProcedure{

    public Delivery(APIDatabase db) {
        super(db);
    }

    public void delivery(int numDistricts, int warehouseID, int carrierID){

        try {
        	db.begin("Delivery");

            for(int districtID = 0; districtID < numDistricts; ++districtID){

                var orderID = getOrderID(warehouseID, districtID);
                if(orderID != null) {
                    deleteOrder(warehouseID, districtID, orderID);
                    var customerID = getCustomerID(warehouseID, districtID, orderID);

                    updateCarrierID(warehouseID, carrierID, districtID, orderID);


                    updateDeliveryDate(warehouseID, districtID, orderID);
                    var orderLineTotal = getOrderLineTotal(warehouseID, districtID, orderID);

                    if(customerID != null)
                        updateBalanceAndDelivery(warehouseID, districtID, customerID, orderLineTotal);


                }
            }


            db.commit();
        } catch (AbortDatabaseException ignored) {
        }


    }

    private Integer getOrderID(int warehouseID, int districtID){

        var newOrderTable = db.readAllIDs(TPCC.NEWORDER);

        var nol = new ArrayList<NewOrder>();
        for(var noSt : newOrderTable){
            if(noSt == null) return null;
            else if(noSt.startsWith(warehouseID+":"+districtID)){
                nol.add(new NewOrder(noSt.replace(":",";")));
            }
        }
        nol.sort((a, b)->(int) (b.getOrderID() - a.getOrderID()));
        return nol.isEmpty() ? null : nol.get(0).getOrderID();
    }

    private void deleteOrder(int warehouseID, int districtID, int orderID) throws AbortDatabaseException {

        db.deleteRow(TPCC.NEWORDER, warehouseID + ":" + districtID + ":" + orderID);
    }

    private Integer getCustomerID(int warehouseID, int districtID, int orderID) throws AbortDatabaseException {

        var oSt = db.readRow(TPCC.OPENORDER,warehouseID+":"+districtID+":"+orderID);
        var ol = oSt == null ? null : new Order(oSt);

        if(ol == null) db.abort(); //TODO: To be fixed when mutex per row.

        return ol.getCustomerID();
    }

    private void updateCarrierID(int warehouseID, int carrierID, int districtID, int orderID) throws AbortDatabaseException {
        var oSt = db.readRow(TPCC.OPENORDER,warehouseID+":"+districtID+":"+orderID);
        var ol = oSt == null ? null : new Order(oSt);

        if(ol == null) db.abort();

        ol.setCarrierID(carrierID);

        db.writeRow(TPCC.OPENORDER, warehouseID+":"+districtID+":"+orderID, ol.toString());

    }

    private void updateDeliveryDate(int warehouseID, int districtID, int orderID) throws AbortDatabaseException {

        var time = System.currentTimeMillis();


        var olALSt = db.readIfIDStartsWith(TPCC.ORDERLINE, warehouseID+":"+districtID+":"+orderID);

        if(olALSt == null) db.abort();

        for(var olSt : olALSt){
            if(olSt == null) db.abort();
            var ol = new OrderLine(olSt);
            ol.setDeliveryDate(time);
            db.writeRow(TPCC.ORDERLINE, ol.getKey(), ol.toString());

        }

    }

    private float getOrderLineTotal(int warehouseID, int districtID, int orderID) throws AbortDatabaseException {

        var amount = 0;

        var olALSt = db.readIfIDStartsWith(TPCC.ORDERLINE, warehouseID+":"+districtID+":"+orderID);

        if(olALSt == null) db.abort();

        for(var olSt : olALSt){
            if(olSt == null) db.abort();
            var ol = new OrderLine(olSt);
            amount += ol.getAmount();

        }

        return amount;
    }

    private void updateBalanceAndDelivery(int warehouseID, int districtID, int customerID, float orderLineTotal)
            throws AbortDatabaseException {

        var cSt = db.readRow(TPCC.CUSTOMER, warehouseID+":"+districtID+":"+customerID);
        var c = cSt == null ? null : new Customer(cSt);

        if(c == null) db.abort();


        c.setBalance(c.getBalance() + orderLineTotal);
        c.setDeliveryCnt(c.getDeliveryCnt() + 1);

        db.writeRow(TPCC.CUSTOMER, warehouseID+":"+districtID+":"+customerID, c.toString());

    }

}
