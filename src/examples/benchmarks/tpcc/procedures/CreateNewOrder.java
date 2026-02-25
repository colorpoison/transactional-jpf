package benchmarks.tpcc.procedures;

import benchmarks.tpcc.TPCC;
import benchmarks.tpcc.TPCCUtility;
import benchmarks.tpcc.objects.*;
import database.AbortDatabaseException;
import database.APIDatabase;

import java.util.function.Function;

public class CreateNewOrder extends BasicTPCCProcedure{


    public CreateNewOrder(APIDatabase db) {
        super(db);
    }

    public void createNewOrder(int warehouseID, int districtID, int customerID,
                                     int orderLineCnt, int allLocal, int[] itemIDs,
                                     int[] supplierWarehouseIDs, int[] orderQuantities){

        try {
            db.begin("CreateNewOrder");


            var c = getCustomerById(warehouseID, districtID, customerID);

            var w = getWarehouse(warehouseID);

            var d = getDistrict(warehouseID, districtID);


            if(d != null) {

                var orderID = d.getNextOrderID();

                updateDistrict(warehouseID, districtID);

                insertOpenOrder(warehouseID, districtID, customerID,
                        orderLineCnt, allLocal, orderID);

                insertNewOrder(warehouseID, districtID, d.getNextOrderID());

                for (int i = 0; i < orderLineCnt; ++i) {
                    int supplyWarehouseID = supplierWarehouseIDs[i];
                    int itemID = itemIDs[i];
                    int quantity = orderQuantities[i];
                    var price = getItemPrice(itemID);
                    var amount = quantity * price;
                    var s = getStock(supplyWarehouseID, itemID, quantity);


                    var ol = new OrderLine(warehouseID, districtID, orderID, i, itemID,
                            supplyWarehouseID, quantity, amount, s.getDistrict(districtID));

                    insertOrderLine(ol);
                    updateStock(s, ol);


                }


            }

            db.commit();
        } catch (AbortDatabaseException ignored) {

        }



    }

    private void insertNewOrder(int warehouseID, int districtID, int nextOrderID) {

        var no = new NewOrder(warehouseID, districtID, nextOrderID);
        db.insertRow(TPCC.NEWORDER, no.getKey());

    }

    private void updateDistrict(int warehouseID, int districtID) throws AbortDatabaseException {
        Function<District, District> f = (District d)->{
            d.setNextOrderID(d.getNextOrderID()+1);
            return d;
        };
        updateDistrict(warehouseID, districtID, f);
    }

    private void insertOpenOrder(int warehouseID, int districtID, int customerID,
                                 int orderLineCnt, int allLocal, int orderId){

        var o = new Order(warehouseID, districtID,orderId,
                customerID, orderLineCnt,allLocal, System.currentTimeMillis());

        db.insertRow(TPCC.OPENORDER, o.getKey(), o.toString());
    }


    private Float getItemPrice(int itemID) throws AbortDatabaseException {

        var iSt = db.readRow(TPCC.ITEM, itemID+"");
        var i = iSt == null ? null : new Item(iSt);

        if(i == null) db.abort();
        return i.getPrice();
    }


    private void insertOrderLine(OrderLine orderLine){

        db.insertRow(TPCC.ORDERLINE, orderLine.getKey(), orderLine.toString());


    }

    private Stock getStock(int warehouseID, int itemID, int quantity) throws AbortDatabaseException {

        var sSt = db.readRow(TPCC.STOCK, warehouseID+":"+itemID);

        var s = sSt == null ? null : new Stock(sSt);

        if(s == null) db.abort();

        var sq = s.getQuantity();
        if (sq - quantity >= 10) {
            s.setQuantity(sq - quantity);
        } else {
            s.setQuantity(sq + -quantity + 91);
        }


        return s;

    }

    private void updateStock(Stock s, OrderLine ol){

        int remoteCntIncrement = ol.getSupplyWarehouseID() == ol.getWarehouseID() ? 0 : 1;
        s.setQuantity(ol.getQuantity());
        s.setYtd(s.getYtd() + ol.getQuantity());
        s.setOrderCnt(s.getOrderCnt() + 1);
        s.setRemoteCnt(s.getRemoteCnt() + remoteCntIncrement);
        db.writeRow(TPCC.STOCK, s.getKey(), s.toString());


    }

}
