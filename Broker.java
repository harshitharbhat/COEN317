import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {


    static Map<String, Set<Integer>> topicSubscriberMap = new ConcurrentHashMap<>();
    public static void main(String[] args) throws IOException {
        int threadId = 0;
        initializeTopicMap();
        int port = Integer.parseInt(args[0]);

        ServerSocket curSocket = new ServerSocket(port);
        System.out.println("Broker on port" + port);


        while (true) {
            Socket socket = null;
            try {
                socket = curSocket.accept();
                System.out.println("Broker id: " + port + " request recieved at : " + socket);
                Thread t = new RequestHandler(socket, ++threadId, port,topicSubscriberMap);
                t.start();
            } catch (Exception e){
                socket.close();
                e.printStackTrace();
            }
        }

    }

    private static void initializeTopicMap() {
        topicSubscriberMap.put("News",new HashSet<>(Arrays.asList(8080)));
        topicSubscriberMap.put("Sports",new HashSet<>(Arrays.asList(8080,8081)));
    }

}

class RequestHandler extends Thread {
//    static Map<String, String> unsentData = new ConcurrentHashMap<>();
    static final Set<Integer> ports = new HashSet<>(Arrays.asList(6000,6001,6002));

    final Socket socket;
    int id;
    int curPort;
    Map<String, Set<Integer>> topicSubscriberMap;

    public RequestHandler(Socket publisherSocket, int threadId, int curPort, Map<String,
            Set<Integer>> topicSubscriberMap) {
        socket = publisherSocket;
        id = threadId;
        this.curPort = curPort;
        this.topicSubscriberMap = topicSubscriberMap;
    }

//    private void sendToOtherBrokers(String key, String data) {
//        for (int port : ports) {
//            Socket socket = null;
//            try {
//                if (port != curPort) {
//                    socket = new Socket(InetAddress.getLocalHost(),port);
//                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
//                    dos.writeUTF("broker " + key + " " + data);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                try {
//                    socket.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

    private void sendToSubscribers(String key, String data) {
        try {
            for(int port : topicSubscriberMap.get(key)) {
                Socket socket = new Socket(InetAddress.getLocalHost(), port);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                dos.writeUTF("broker " + key + " " + data);
                socket.close();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void run() {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            System.out.println("Request recieved");
            String data = dis.readUTF();
            String[] splitData = data.split(" ");
            System.out.println(splitData[0]);
            switch (splitData[0]) {
                case "pub":
//                    sendToOtherBrokers(brokenDown[1], brokenDown[2]);
                    sendToSubscribers(splitData[1], splitData[2]);
                    break;
                case "sub":
                    topicSubscriberMap.get(splitData[1]).add(Integer.valueOf(splitData[2]));
                    break;
              case "Liveness":
                  System.out.println("Liveness check");
                  dos = new DataOutputStream(socket.getOutputStream());
                  dos.writeUTF("OK");
                  break;

            }

            dis.close();
            dos.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}