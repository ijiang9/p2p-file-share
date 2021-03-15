
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
public interface InitControl extends Remote {

	//Server interface

	public ArrayList<String> getNeighbour(String peerName) throws RemoteException;
	public boolean getPush() throws RemoteException;
	public boolean getPoll() throws RemoteException;
	
}
