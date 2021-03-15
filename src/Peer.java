
import java.rmi.*;
public interface Peer extends Remote {
	//Peer Interface
	public byte[] downloadService(String fileName) throws RemoteException;
	public void retrieve(String peer, String fileName) throws RemoteException;
	public void query(String messageID, int TTL, String filename, String upStream) throws RemoteException;
	public void invalidate(String messageID, String originID, String filename, Integer versionNum) throws RemoteException;
	public void queryHit(String messageID, int TTL, String filename, String peerName) throws RemoteException;
	public long concurrentTest() throws RemoteException;
	public void addToIndex(String peerName, String filename, Integer versionNum, String d, String originID) throws RemoteException;
}
