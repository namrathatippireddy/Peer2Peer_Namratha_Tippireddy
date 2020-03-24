import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.ConcurrentHashMap;

public class Peer { // this class has the funtionality of peer as Client stuff
	Socket requestSocket; // socket connect to the FilerOwner or DownloadNegibour to downlaod chunks from
	ServerSocket peerAsServerSocket; // socket on which other peers can connect on to this peer and treat this peer as server
	Socket connSocket;
	private static int sPort = 0, oPort=0, dPort=0;
	ObjectOutputStream out; // stream write to the socket
	ObjectInputStream in; // stream read from the socket
	DataInputStream din;
	PeerLogger logger;
	String message; // message send to the server
	String peerID;  // Ideally should be same as the port number that this peer exposes as server himself
	String fileOwnerID = "";
	String downloadNeighbourID = "";
	String server = "FileOwner"; //This is being hard coded and used everywhere, this should be transtitioned to download neighbour dependingly
	ConcurrentHashMap<String, Boolean> chunkLedger = new ConcurrentHashMap<String, Boolean>(); // to keep track of what is downloaded and what is not, false being not dowladed yet
	int totalChunkCounterFromFileOwner = 0; // ToDO: this gets updatedin communicate_with_FileOwner with the total count of chunks

	public Peer(String peerID) {
		this.peerID = peerID;
		this.logger = new PeerLogger(peerID);
	}

	// send a message to the output stream
	public void sendMessage(String msg) {
		try {
			// stream write the message
			out.writeObject(msg);
			out.flush();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
	}

	public ArrayList<String> receiveChunkListAndProcess(String dneighbor){
		logger.receiveChunkList(dneighbor);
		System.out.println("Received ChunkList from "+dneighbor);
		ArrayList<String> possibleChunksToRequest = new ArrayList<String>(); // This is the Inersection list of  "ChunksNonPresent in peer" with "ChunksListReceived to choose a chunk"
		try {
			// Receive chunkList and populate the chunkLedger
			// System.out.println("Inside the method that receives chunkList from server");
			String crudeChunkList = (String)in.readObject();
			// System.out.println("ChunkList sent by server :\n"+ crudeChunkList);
			String[] chunklist = crudeChunkList.split("\n");

			for(String chunkNum:chunklist){
				if (! chunkLedger.containsKey(chunkNum)) { 
					chunkLedger.put(chunkNum, false); // this value would later set to true, meaning that the download is done.
        		}
        		if (!chunkLedger.get(chunkNum)){
        			possibleChunksToRequest.add(chunkNum);
        		}
			}
		}
		catch(Exception e) {
			System.out.println("Exception receiving the chunk list");
			e.printStackTrace();
        	System.out.println("Exception: "+e);
		}

		return possibleChunksToRequest;
	}

 	public void receiveChunkFromServer(String chunkToReceive) {
        try {
            int bytesRead;
            String chunkName = din.readUTF();
            System.out.println("FileName:"+ chunkName);
            if(chunkName.equals("FileNotFound")) {
            	System.out.println("Chunk '"+ chunkToReceive +"' not found on server.");
            }
            else {
	            // This output is to write the file into the buffer 
	            OutputStream output = new FileOutputStream("./file/chunks/"+chunkName);
	            long size = din.readLong();
	            byte[] buffer = new byte[1024];
	            while (size > 0 && (bytesRead = din.read(buffer, 0, (int) Math.min(buffer.length, size))) != -1) {
	                output.write(buffer, 0, bytesRead);
	                size -= bytesRead;
	            }
	            System.out.println("Chunk "+chunkToReceive+" received from Server.");
	            // Now update the ledger saying, this particular chuck is dowloaded
	            chunkLedger.put(chunkToReceive, true);
	            System.out.println("chunkLedger =======> "+ chunkLedger.toString());
	            logger.downloadedChunkNum(server, chunkToReceive);
            }
        }
        catch (EOFException eof_ex) {
        	System.out.println("Chunk not found on server");
        }catch (Exception ex) {
        	System.out.println("Chunk not received");
       	}  
	}

	public void receiveTotalChunkNumFromServer()
	{
		try{
			totalChunkCounterFromFileOwner = Integer.parseInt((String)in.readObject());
			System.out.println("total_chunks to be received is "+ totalChunkCounterFromFileOwner);
		}
		catch(Exception exe){
			System.out.println("Exception in receiving the total num of chunks");
			exe.printStackTrace();
		}
	}


	public boolean isDownloadDone(){  //** make sure Chunk list is retrived atleast once, should be thread safe
		int chunksPresentCounter = numberOfDownlodedChunks();
		if( totalChunkCounterFromFileOwner == chunksPresentCounter){
			return true;
		}
		return false;
	}

	public int numberOfDownlodedChunks(){ // this uses thread safe way of reading the concurrent hashmap contents
		Iterator<String> itr = chunkLedger.keySet().iterator();
		int chunksPresentCounter = 0;
		synchronized (chunkLedger) 
		{
		    while(itr.hasNext()) {
		        if(chunkLedger.get(itr.next())){
		        	chunksPresentCounter++;
		        }
		    }
		}
		return chunksPresentCounter;
	}



	// method to take Establishing connection to the server be it FileOwner or DownloadNeighbour
	public void establishServerConnection(int port) throws InterruptedException{
		boolean retryConnecting = true;
		while(retryConnecting)
		{
			try {
				// create a socket to connect to the server be it Fileowner or DownloadNeigbour
				requestSocket = new Socket("localhost", port); 
				System.out.println("Connected to localhost in port" +port);
				// initialize inputStream and outputStream
				out = new ObjectOutputStream(requestSocket.getOutputStream());
				out.flush();
				in = new ObjectInputStream(requestSocket.getInputStream());
				din = new DataInputStream(requestSocket.getInputStream());
				retryConnecting = false;
			}
			catch(ConnectException e)
			{
				System.out.println("Couldn't connect, Retrying to connect to "+port+"...");
			    Thread.sleep(2000);
				retryConnecting = true;
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// method to take Establishing connection to the server be it FileOwner or DownloadNeighbour
	public void closeServerConnection(){
		try {
			in.close();
			out.close();
			din.close();
			requestSocket.close();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
	}


	public void communicate_with_FileOwner(int port){  // this port number has to be FileOwner always
		try {
			establishServerConnection(port);
			String msg ="";
			
			// 1. client sending 5001:Ready
			msg = "Ready";
			sendMessage(peerID+":"+msg);
			logger.sendMsg(server, msg);

			// 2. client sending 5001:ChunkList, Server only sends the list of chunks intended for this peer, then process response
			msg = "ChunkList";
			sendMessage(peerID+":"+msg);
			logger.requestChunkList(server);
			ArrayList<String> chunksListToChooseFrom = receiveChunkListAndProcess(server);
			//System.out.println("Chunk to randomly chose from:"+ Arrays.toString(chunksListToChooseFrom.toArray()));

			// 3. client sending 5001:Chunk:1, this is done for all the chunks that intended to send to this peer 
			// receive the requested chunks and update the chunkLedger
			boolean gotAllFromFOwner = false;
			while (!gotAllFromFOwner) {
				msg = "Chunk";
				for (String chunkNumberSelected : chunksListToChooseFrom) {
					sendMessage(peerID+":"+msg+":"+ chunkNumberSelected);
					logger.requestChunkNum(server, chunkNumberSelected);
					receiveChunkFromServer(chunkNumberSelected);
				}
				if(chunksListToChooseFrom.size() == numberOfDownlodedChunks()){
					gotAllFromFOwner = true;
				}
			}

			//4. Get the total number of chunks that we need to make the file
			msg="TOTALCHUNKS";
			sendMessage(peerID+":"+msg);
			receiveTotalChunkNumFromServer();

		} catch (Exception e) {
			System.err.println("Connection refused. You need to initiate a server first.");
		}finally {
			// Close connections
			closeServerConnection();
		}
	}


	public void communicate_with_Downloadneighbour(int port){  // this port number is transtitioned between FileOwner and Download neighbour
		try {

			establishServerConnection(port); // re-establishs connection on the Downloadneighbour port

			String msg ="";
			boolean doneWithAllChunks = false;

			while (!doneWithAllChunks) {
				// 1. client sending 5001:Ready
				msg = "Ready";
				sendMessage(peerID+":"+msg);
				logger.sendMsg(server, msg);

				// 2. client sending 5001:ChunkList, Server only sends the list of chunks intended for this peer, then process response
				msg = "ChunkList";
				sendMessage(peerID+":"+msg);
				logger.requestChunkList(Integer.toString(port));
				ArrayList<String> chunksListToChooseFrom = receiveChunkListAndProcess(Integer.toString(port));
				//System.out.println("Chunk to randomly chose from:"+ Arrays.toString(chunksListToChooseFrom.toArray()));
				if(chunksListToChooseFrom.size() == 0){ 
					logger.hasSameChunkList(server);
					Thread.sleep(1000);
					continue;
				}

				// 3. client sending 5001:Chunk:1, this is done for all the chunks that peer needs to download
				// receive the requested chunks and update the chunkLedger

				// ** following random chunk selection thing is needed while comunicating with peer acting as server
				// Now select a random chunk number from the ones that aren't downloded yet (chunksListToChooseFrom)
				Random rand = new Random();
				int randomChunkIndex = rand.nextInt(chunksListToChooseFrom.size());
				String chunkNumberSelected = chunksListToChooseFrom.get(randomChunkIndex);
				msg = "Chunk";
				sendMessage(peerID+":"+msg+":"+ chunkNumberSelected);
				logger.requestChunkNum(Integer.toString(port), chunkNumberSelected);
				receiveChunkFromServer(chunkNumberSelected);

				// 4. check to see whether all the chunks are received and print done
				// LastStep: when the recevied_chunk_counter reaches total_chunk_counter attempt file merge, this block goes into Run too
				// This doesn't have to be called while communicating with server, should be done while communicating with downlod neighbour
				if(isDownloadDone()){
					String fileName="test.pdf"; // hardcoding this merged file too for now (It should actually be the same name as that of server)
					PeerUtils.mergeFile(chunkLedger.size(), fileName);
					logger.doneMerging();
					doneWithAllChunks = true;
				}
			}
		} catch (Exception e) {
			System.err.println("Connection refused. You need to initiate a server first.");
		}finally {
			// Close connections
			closeServerConnection();
		}
	}

	public void triggerPeerAsServer(int portPeerActAsSever){
		try{
			peerAsServerSocket = new ServerSocket(portPeerActAsSever);
			System.out.println("Peer now acts as DownloadNeighbour, running on port ["+ portPeerActAsSever +"]");

			int clientNum = 1;
			while (true) {
				if(clientNum < 2){
					connSocket = peerAsServerSocket.accept();
					new PeerAsServer(connSocket, clientNum, logger).start();
					System.out.println("Client " + clientNum + " is connected!");		
					clientNum++;
				}
			}
		} catch (Exception e) {
			System.err.println("Exception occured at function triggerPeerAsServer");
		} finally {
			try{
				peerAsServerSocket.close();
			}
			catch (Exception e) {
				System.err.println("Exception occured at closing peerAsServerSocket");
			} 
		}	
	}

	// main method
	public static void main(String args[]) throws Exception {
        if (args.length > 0)
		{
			//serveraddr = args[0];
			sPort = Integer.parseInt(args[0]);
			oPort = Integer.parseInt(args[1]);
			dPort = Integer.parseInt(args[2]);
		}
		// peer is now active and will be communicating with server first
		Peer peer = new Peer(Integer.toString(oPort));
		// 1. Now the server be FileOwner and get done will all the chunks it needs to get from the FileOwner once for all
		peer.communicate_with_FileOwner(sPort); // as the FileOwner always runs on 5000

		// 2. Now Publish this peer as a server to the rest of the peers on a seperate thread, and will be executing cocurrently
		Thread peerAsServerThread = new Thread(new Runnable(){ 
										public void run(){
											peer.triggerPeerAsServer(oPort); // the current peer acting as server on the port 
										}
									});
		peerAsServerThread.start();

		// 3. Now the server be Downloadneighbour and get done when all the other chunks are downloaded 
		peer.server = Integer.toString(dPort); // for logging purposes, now the server changes from FileOwner to DownloadNeighbour[ hardcoded to 5002]
		peer.communicate_with_Downloadneighbour(dPort); // for now hardcoded to 5002
	}
}


class PeerAsServer extends Thread {
	private String message; // message received from the client
	private Socket connection;
	private ObjectInputStream in; // stream read from the socket
	private ObjectOutputStream out; // stream write to the socket
	private DataOutputStream dos;
	private int no; // The index number of the client
	private PeerLogger logger;

	public PeerAsServer(Socket connection, int no, PeerLogger logger) {
		this.connection = connection;
		this.logger = logger;
		this.no = no;
	}

	public void run() {
		try {
			// initialize Input and Output streams
			out = new ObjectOutputStream(connection.getOutputStream());
			dos = new DataOutputStream(connection.getOutputStream());
			dos.flush();
			out.flush();
			in = new ObjectInputStream(connection.getInputStream());
			try {
				while (true) {
					// 3. Now read the incoming msg understand the request: Send ChunkLink/ Send Chunk
					// receive the message sent from the client
					String[] msgFromClient = ((String) in.readObject()).split(":");	// msg format be liks => 5001:Ready or 5001:ChunkList or 5001:Chunk:1
					String currentPeer = msgFromClient[0];
					String msg = msgFromClient[1].toUpperCase();

					if(msg.equals("READY")){
						logger.receivedMsg(currentPeer, msg);
					} else if(msg.equals("CHUNKLIST")){
						collectAndSendChunkList(currentPeer);
					} else if(msg.equals("CHUNK")){
						String chunkNumToSend = msgFromClient[2];
						// sending  the chunk to client  logic gets called here
						logger.receiveRequestChunkNum(currentPeer);
						System.out.println("Received request to send chunk "+chunkNumToSend);
						sendChunkToPeer(currentPeer, chunkNumToSend);
					}
					
				}
			} catch (ClassNotFoundException classnot) {
				System.err.println("Data received in unknown format");
			}
		} catch (IOException ioException) {
			System.out.println("Disconnect with Client " + no);
		} finally {
			// Close connections
			try {
				in.close();
				out.close();
				dos.close();
				connection.close();
			} catch (IOException ioException) {
				System.out.println("Disconnect with Client " + no);
			}
		}
	}

	public synchronized void collectAndSendChunkList(String peerID){
		try {
			File chunksDirPath = new File("./file/chunks");
			if (!chunksDirPath.isDirectory()) {
				System.out.println("Source directory is not valid");
			}
			
			File[] chunksList = chunksDirPath.listFiles(); 
			int filecount = chunksList.length;
			String chunkNamesList = "";
			for (int i=0; i<filecount; i++)
			{
				chunkNamesList = chunkNamesList+(chunksList[i].getName())+"\n"; 
				//System.out.println("Sent file: " +files[i].getName());
			}
			
			out.writeObject(chunkNamesList);
			out.flush();
			logger.sentChunkList(peerID);

		}
		catch(Exception e) {
			System.out.println("FileOwner couldn't give the chunks list");
			e.printStackTrace();
        	System.out.println("Exception: "+e);
		}
	}	

	public synchronized void sendChunkToPeer(String peerId, String chunkNum) {
		try {
    		File reqChunk = new File("./file/chunks/"+chunkNum);  //search for the required file and throw an error if its not there
    		System.out.println("Chunk exist: "+reqChunk.exists());
    		if (!reqChunk.exists())
    		{	
    			dos.writeUTF("FileNotFound");
    			dos.flush();
    			System.out.println("Chunk '" + chunkNum +"' doesn't exist on server ");
    		}
    		else {
	           // System.out.println("File length in bytes is "+reqChunk.length());
	            FileInputStream fileIS = new FileInputStream(reqChunk);
		        int count = fileIS.available();
	            // System.out.println("Chunk length is "+count);
	            DataInputStream dataIS = new DataInputStream(fileIS);
	            byte[] servbuffer = new byte[count];
	            dataIS.read(servbuffer);
	            dos.writeUTF(reqChunk.getName());
	            dos.writeLong(servbuffer.length);
	            dos.write(servbuffer, 0, servbuffer.length);
	            dos.flush();
	            System.out.println("Chunk "+chunkNum+" is sent to the requesting client");
	            logger.uploadedChunkNum(peerId, chunkNum);
    		}
		}
		catch(Exception exe) {
			exe.printStackTrace();
		} 
	}
}
