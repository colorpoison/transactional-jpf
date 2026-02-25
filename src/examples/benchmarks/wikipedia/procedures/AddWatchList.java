package benchmarks.wikipedia.procedures;

import benchmarks.wikipedia.Wikipedia;
import benchmarks.wikipedia.objects.WatchList;
import database.APIDatabase;

public class AddWatchList extends WikipediaProcedure {

    public AddWatchList(APIDatabase db) {
        super(db);
    }

    public void addWatchList(int userID, int nameSpace, String pageTitle) {
        db.begin("AddWatchList");

        if(userID > 0) {

            insertWatchList(userID, nameSpace, pageTitle);

            if (nameSpace == 0) {
                insertWatchList(userID, 1, pageTitle);
            }

            updateUserTouched(userID);
        }
        db.commit();
    }

    private void insertWatchList(int userID, int nameSpace, String pageTitle){

        var w = new WatchList(userID, nameSpace, pageTitle, null);

        db.insertRow(Wikipedia.WATCHLIST, userID +":"+nameSpace+":"+pageTitle, w.toString());
    }


}
