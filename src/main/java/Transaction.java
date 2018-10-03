import java.util.Properties;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;

public class Transaction {
	private Seats dao;
	private int insID;
	private Properties p;

	// constructor
	public Transaction(int insID, Properties p) {
		this.insID = insID;
		dao = new Seats(insID);
		this.p = p;

	}

	// run the transaction wrapper
	public void run(String opType) {

		try {
			if (opType.equals("updateReservation")) {
				int c_id = Integer.valueOf(p.getProperty("c_id"));
				dao.updateReservation(c_id*1111, 1, c_id, 13, 1, 666);
			} else if (opType.equals("deleteReservation"))
				dao.deleteReservation();
			else if (opType.equals("init"))
				dao.initialize();
			else if (opType.equals("updateCustomer")) {
				int c_id = Integer.valueOf(p.getProperty("c_id"));
				dao.updateCustomer(null, "kiarash", 10L, 777, 666);
			}
			else if (opType.equals("newReservation"))
				dao.newReservation();
			else if (opType.equals("findOpenSeats"))
				dao.findOpenSeats();
			else if (opType.equals("findFlights"))
				dao.findFlights();
			else
				System.err.println("Unknown Operaition Type");
		} catch (MySQLTransactionRollbackException e) {
			// retry
			try {
				Thread.sleep(10);
				// System.out.println(".");
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			run(opType);
		} catch (Exception e) {
			System.out.println(e);
			// TODO Auto-generated catch block

		}
	}
}
