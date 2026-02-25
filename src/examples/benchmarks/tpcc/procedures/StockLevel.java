package benchmarks.tpcc.procedures;

import benchmarks.tpcc.TPCC;
import benchmarks.tpcc.TPCCUtility;
import benchmarks.tpcc.objects.District;
import benchmarks.tpcc.objects.OrderLine;
import benchmarks.tpcc.objects.Stock;
import database.AbortDatabaseException;
import database.APIDatabase;

import java.util.ArrayList;


public class StockLevel extends BasicTPCCProcedure{

    public StockLevel(APIDatabase db) {
        super(db);
    }

    //districtID and threshold are random!!!
    public Integer stockLevel( int warehouseID, int districtID, int threshold){

        try {
            db.begin("StockLevel");
            Integer orderID = getOrderID(warehouseID, districtID);
            var stockCount = getStockCount(warehouseID, districtID, threshold, orderID);
            db.commit();
            return stockCount;
        } catch (AbortDatabaseException ignored) {

        }
        return -1;

    }

    private Integer getStockCount(int warehouseID, int districtID,
                                  int threshold, int orderID) throws AbortDatabaseException {

        int count = 0;
        var ols = db.readIfIDStartsWith(TPCC.ORDERLINE, warehouseID+":"+districtID);

        for(var olSt: ols){

            var ol = olSt == null ? null : new OrderLine(olSt);
            if(ol == null) continue;

            var stString = db.readRow(TPCC.STOCK, warehouseID+":"+ ol.getItemID());
            if(stString == null) continue;
            var st = new Stock(stString);


            if(ol.getOrderID() < orderID && ol.getOrderID() >= orderID - 20
                    && st.getQuantity() < threshold){
                ++count;
            }

        }
        if(count == 0) db.abort();

        return count;
    }

    private Integer getOrderID(int warehouseID, int districtID) throws AbortDatabaseException {
        var dSt = db.readRow(TPCC.DISTRICT, warehouseID+":"+districtID);
        var d = dSt == null ? null : new District(dSt);

        if(d == null) db.abort();
        return d.getNextOrderID();
    }
}
