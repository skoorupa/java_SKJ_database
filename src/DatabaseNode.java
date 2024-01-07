import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class DatabaseNode {
    static int tcpport = 10000;
    static HashMap<String, String> records = new HashMap<>();
    static ArrayList<String> connect_ips = new ArrayList<>();
    static ServerSocket server;
    static ArrayList<String> connections = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport":
                    tcpport = Integer.parseInt(args[++i]);
                    break;
                case "-record":
                    String[] split = args[++i].split(":");
                    records.put(split[0], split[1]);
                    break;
                case "-connect":
                    connect_ips.add(args[++i]);
                    break;
            }
        }

        server = new ServerSocket(tcpport);
        System.out.println("[N]: Running new node at port: "+tcpport);
        System.out.println("[N]: Records:");
        for (String key : records.keySet()) {
            System.out.println(key+": "+records.get(key));
        }

        for (String ip : connect_ips) {
            connections.add(ip);
            System.out.println("[N]: Connecting to node: "+ip);
            // connect to these todo
            String[] split = ip.split(":");
            Socket node = new Socket(split[0], Integer.parseInt(split[1]));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));

            bw.write("node-join "+tcpport);
            bw.newLine();
            bw.flush();
            node.close();
            System.out.println("[N]: Connected successfully");
        }

        boolean terminate = false;
        while (!terminate) {
            Socket hello = server.accept();
            String helloIP = hello.getInetAddress()+":"+hello.getPort();
            BufferedReader br = new BufferedReader(new InputStreamReader(hello.getInputStream()));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hello.getOutputStream()));
            System.out.println("[N]: New connection "+helloIP);

            String request = br.readLine();
            System.out.println("[N]: Got command from "+helloIP+" : "+request);

            String command = request;
            if (request.indexOf(' ') != -1) command = request.substring(0, request.indexOf(' '));

            switch (command) {
                case "node-join": {
                    String host = hello.getRemoteSocketAddress().toString().substring(1,hello.getRemoteSocketAddress().toString().indexOf(':'));
                    String nodeIP = host+":"+request.substring(request.indexOf(' ') + 1);
                    System.out.println("[N]: New node joined: "+nodeIP);
                    connections.add(nodeIP);
                    break;
                }
                case "get-value": {
                    String arg = request.substring(request.indexOf(' ') + 1);
                    if (records.containsKey(arg))
                        bw.write(arg + ":" + records.get(arg));
                    else {

                        bw.write("ERROR");
                    }
                    break;
                }
                case "new-record": {
                    String arg = request.substring(request.indexOf(' ') + 1);
                    String[] split = arg.split(":");
                    records.put(split[0], split[1]);
                    bw.write("OK");
                    break;
                }
                case "terminate": {
                    terminate = true;
                    bw.write("OK");
                }
                default:
                    System.out.println("[N]: Error: could not process command: "+command);
            }

            bw.flush();
            hello.close();

            System.out.println("[N]: Ended connection with "+helloIP);
        }
    }
}
