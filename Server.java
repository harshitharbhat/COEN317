import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class Server {

    static ConcurrentHashMap<Integer,Integer> processes= new ConcurrentHashMap<Integer,Integer>();
    static ConcurrentHashMap<Integer,Boolean> serverMap= new ConcurrentHashMap<Integer,Boolean>();
    static ArrayList<Integer> ports = new ArrayList<>(Arrays.asList(6000,6001,6002));
    static int currentBroker = 6000;

    static void initializeMaps(){
        int processId = 1;
        for(int p : ports){
            processes.put(processId, p);
            serverMap.put(p,true);
            processId++;
        }
    }


    public static void main(String[] args) throws UnknownHostException {
        System.out.println("Server starting");
        initializeMaps();
        InetAddress ip = InetAddress.getLocalHost();

        Thread thread = new HeartBeat(ip,currentBroker,processes,serverMap);
        thread.start();
    }
}

class HeartBeat extends Thread{

    DataInputStream dis;
    DataOutputStream dos;
    InetAddress ip;
    int currentLeader;
    int currentBrokerId;
    ConcurrentHashMap<Integer,Integer> brokers;
    ConcurrentHashMap<Integer,Boolean> brokerStatuses;

    public HeartBeat(InetAddress ip, int currentLeader, ConcurrentHashMap<Integer,Integer> brokers,
                     ConcurrentHashMap<Integer,Boolean> brokerStatuses ){
        this.ip = ip;
        this.currentLeader = currentLeader;
        this.brokers = brokers;
        this.brokerStatuses = brokerStatuses;
    }
    public void run(){
        System.out.println("run method");
        while (true){
            try {
                Socket brokerSocket = new Socket(ip, currentLeader);
                dis = new DataInputStream(brokerSocket.getInputStream());
                dos = new DataOutputStream(brokerSocket.getOutputStream());
                System.out.println("checking here");
                dos.writeUTF("Liveness");
                String response = dis.readUTF();

                if(response.equals("OK")){
                    System.out.println("Current Broker " + currentLeader);
                }
                Thread.sleep(10000);
            } catch (IOException e) {
                System.out.println("Broker down " + currentLeader);
                brokerStatuses.put(currentLeader,false);
                currentLeader = brokers.get(electLeader());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public int electLeader(){
        System.out.println("Election initiated..");
        int highestProcess = 1;

        for(int processId : brokers.keySet()){
            if(processId > currentBrokerId){
                int destServer = brokers.get(processId);
                if(brokerStatuses.get(destServer)){
                    System.out.println("Election request sent to server at : " + destServer);
                    highestProcess = Math.max(highestProcess, processId);
                }
            }
        }
        System.out.println("Leader elected as :" + highestProcess);
        return highestProcess;
    }
}