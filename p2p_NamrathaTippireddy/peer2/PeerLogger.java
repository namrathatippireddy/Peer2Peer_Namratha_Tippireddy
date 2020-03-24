
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;



public class PeerLogger {

	private PrintWriter logfile;
	private String src;
	private String logString="";

	public PeerLogger(String src) {
		try {
			this.src = src;
			this.logfile = new PrintWriter("logfile_"+src + ".log");
		} catch (FileNotFoundException e) {
			System.out.println("Not able to create log writer");
		}
	}

	public void doneSplitting(int numOfChunks) {
		try {
			logString = "["+this.src+"]"+" Finished splitting file in to number of chunks:" + "["+numOfChunks+"]";
			printAndFlush();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void doneMerging() {
		try {
			logString = "["+this.src+"]"+" Received all chunks of the file and finished merging the file";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void receivedConnection(String peerID) {
		try {
			logString = "["+this.src+"]"+" received connection from peer" + "["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void retryConnecting(String peerID) {
		try {
			logString = "Retrying connection to DownloadNeighbor at port " +  "["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendMsg(String peerReceiving, String msg) {
		try {
			logString = "["+this.src+"]"+" sends Message: "+ msg + " to ["+peerReceiving+"]";
			printAndFlush();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void receivedMsg(String peerSending, String msg) {
		try {
			logString = "["+this.src+"]"+" received Message: "+msg + " from peer ["+peerSending+"]";
			printAndFlush();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void requestChunkList(String peerToResponsd) {
		try {
			logString = "["+this.src+"]"+" request Chunk list from ["+peerToResponsd+"]";
			printAndFlush();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sentChunkList(String peerID) {
		try {
			logString = "["+this.src+"]"+" sent Chunk List to peer ["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void receiveRequestChunkNum(String chunkNum) {
		try {
			logString = "["+this.src+"]"+" Request Chunk ["+chunkNum +"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void receiveChunkList(String peerID) {
		try {
			logString = "["+this.src+"]"+" received Chunk List from peer ["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sameChunkList(String peerID) {
		try {
			logString = "["+this.src+"]"+" has same Chunk list as the DownloadNeighbour ["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void requestChunkNum(String peerToResponsd, String chunkNum) {
		try {
			logString = "["+this.src+"]"+" Request Chunk ["+chunkNum +"] from ["+peerToResponsd+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void uploadedChunkNum(String peerID, String chunkNum) {
		try {
			logString = "["+this.src+"]"+" Uploaded Chunk ["+chunkNum +"] to peer ["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void downloadedChunkNum(String peerID, String chunkNum) {
		try {
			logString = "["+this.src+"]"+" Download Chunk ["+chunkNum +"] from ["+peerID+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void hasSameChunkList( String downloadNeigbhour){
		try {
			logString = "Peer ["+this.src+"]"+" has the same ChunkList as that of DownloadNeigbhour ["+downloadNeigbhour+"]";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void finallyThanks(String peerID, String chunkNum) {
		try {
			logString = "["+this.src+"]"+" Thanks!!! Received all chunks.";
			printAndFlush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void printAndFlush(){
		this.logfile.println(logString);
		System.out.println(logString);
		this.logfile.flush();
	}
}
