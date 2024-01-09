import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class DatabaseNode {
    static boolean terminate = false;
    static int tcpport = 10000;
    static ArrayList<Thread> threads = new ArrayList<>();
    static ArrayList<Socket> sockets = new ArrayList<>();
    static ServerSocket server;
//    static HashMap<String, String> records = new HashMap<>();
    static String key = "";
    static String val = "";
    static ArrayList<String> connect_ips = new ArrayList<>();
    static HashMap<String,String> nodeIPs = new HashMap<>(); // K - node IP, V - moj IP u innych
    static HashSet<String> currentRequests = new HashSet<>();

    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport":
                    tcpport = Integer.parseInt(args[++i]);
                    break;
                case "-record":
                    String[] split = args[++i].split(":");
                    key = split[0];
                    val = split[1];
                    break;
                case "-connect":
                    connect_ips.add(args[++i]);
                    break;
            }
        }

        try {
            server = new ServerSocket(tcpport);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        System.out.println("[N]: Running new node at port: "+tcpport);
        System.out.println("[N]: Record: "+key+":"+val);

        for (String ip : connect_ips) {
            System.out.println("[N]: Connecting to node: "+ip);

            Socket node = null;
            String destination = "";
            try {
                node = new Socket(getHost(ip), getPort(ip));
                sockets.add(node);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                BufferedReader br = new BufferedReader(new InputStreamReader(node.getInputStream()));
//            connections.add(ip);

                bw.write("NODE-JOIN "+tcpport+"|"+ip);
                bw.newLine();
                bw.flush();
                destination = br.readLine();
                nodeIPs.put(ip,destination);
                System.out.println("[N]: Connected successfully: saved "+ip+", he sees me as "+destination);

                node.close();
            } catch (IOException e) {
                System.err.println("[N]: Server closed...");
            }

            sockets.remove(node);
        }
        while (!terminate) {
            Socket hello;
            try {
                hello = server.accept();
                Socket finalHello = hello;
                Thread t = new Thread(()-> {
                    try {
                        acceptSocket(finalHello);
                    } catch (IOException e) {
                        System.err.println("[N]: Socket closed...");
                    }
                });
                t.start();
                threads.add(t);
            } catch (IOException e) {
                System.err.println("[N]: Server closed...");
            }
        }
    }

    public static String getHost(String IP) {
        return IP.substring(0,IP.indexOf(':'));
    }

    public static int getPort(String IP) {
        return Integer.parseInt(IP.substring(IP.indexOf(':')+1));
    }

    public static void acceptSocket(Socket hello) throws IOException {
        sockets.add(hello);
        String helloIP = hello.getInetAddress()+":"+hello.getPort();
        BufferedReader br = new BufferedReader(new InputStreamReader(hello.getInputStream()));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hello.getOutputStream()));
        System.out.println("[N]: New connection "+helloIP);

        String request = br.readLine();
        System.out.println("[N]: Got command from "+helloIP+" : "+request);

        String command = request;
        if (request.indexOf(' ') != -1) command = request.substring(0, request.indexOf(' '));
        HashSet<String> askedNodes = new HashSet<>();

        if (command.equals("NODE-ASK")) {
            // node-ask sourceIP
            // get-value ...
            // dodaj IP do listy odpytanych
            String nodeIP = request.substring(request.indexOf(' ')+1);
            request = br.readLine();

            if (isRequestProcessed(request)) {
                System.out.println("[N]: My request got back to me! Sending ERROR");
                bw.write("ERROR");
                bw.newLine();
                bw.flush();
                hello.close();
                sockets.remove(hello);

                System.out.println("[N]: Ended connection with "+helloIP);
                return;
            }
            askedNodes.add(nodeIP);
            if (request.indexOf(' ') != -1) command = request.substring(0, request.indexOf(' '));
            System.out.println("[N]: Command from node: "+request);
        }
        switch (command) {
            case "NODE-JOIN": {
                // ja tu dostaje tylko tcp port
                String host = getHost(hello.getRemoteSocketAddress().toString());
                String sourceIP = host+":"+request.substring(request.indexOf(' ') + 1, request.indexOf('|')); // node IP
                sourceIP = sourceIP.replace("/",""); // remove "/" at the beginning
                String destinationIP = request.substring(request.indexOf('|')+1); // moj IP u noda

                synchronized (nodeIPs) {
                    nodeIPs.put(sourceIP,destinationIP);
                }
                System.out.println("[N]: New node joined: "+sourceIP+", he sees me as: "+destinationIP);
                bw.write(sourceIP);
                break;
            }
            case "NODE-BYE": {
                String nodeIP = request.substring(request.indexOf(' ')+1);
                nodeIPs.remove(nodeIP);
                break;
            }
            case "get-value": {
                String arg = request.substring(request.indexOf(' ') + 1);
                String wantedkey = arg;
                synchronized (key) {
                    if (key.equals(wantedkey)) {
                        System.out.println("[N]: Found record: "+wantedkey + ":" + val);
                        bw.write(wantedkey + ":" + val);
                    } else {
                        System.out.println("[N]: Cannot find record "+wantedkey+", will ask other nodes!");
                        synchronized (currentRequests) {
                            currentRequests.add(request);
                        }
                        boolean found = false;
                        for (String nodeIP : nodeIPs.keySet()) {
                            if (askedNodes.contains(nodeIP)) continue;
                            System.out.println("[N]: Asking "+nodeIP);
                            Socket node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                            BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                            nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                            nodebw.newLine();
                            nodebw.flush();
                            nodebw.write(command+" "+arg);
                            nodebw.newLine();
                            nodebw.flush();
                            String response = nodebr.readLine();
                            if (!Objects.equals(response, "ERROR")) {
                                bw.write(response);
                                found = true;
                                System.out.println("[N]: Found record at "+nodeIP+"! Response is: "+response);
                                break;
                            } else askedNodes.add(nodeIP);
                        }
                        synchronized (currentRequests) {
                            currentRequests.remove(request);
                        }
                        if (!found) {
                            bw.write("ERROR");
                            System.out.println("[N]: Could not find record "+arg);
                        }
                    }
                }
                break;
            }
            case "set-value": {
                String arg = request.substring(request.indexOf(' ') + 1);
                String wantedkey = request.substring(request.indexOf(' ') + 1, request.indexOf(':'));
                String newvalue = request.substring(request.indexOf(':') + 1);
                synchronized (key) {
                    if (key.equals(wantedkey)) {
                        System.out.println("[N]: I have record: "+wantedkey + ", changing value to: "+newvalue);
                        val = newvalue;
                        bw.write("OK");
                    } else {
                        System.out.println("[N]: Cannot find record "+wantedkey+", will ask other nodes!");
                        synchronized (currentRequests) {
                            currentRequests.add(request);
                        }
                        boolean found = false;
                        for (String nodeIP : nodeIPs.keySet()) {
                            if (askedNodes.contains(nodeIP)) continue;
                            System.out.println("[N]: Asking "+nodeIP);
                            Socket node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                            BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                            nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                            nodebw.newLine();
                            nodebw.flush();
                            nodebw.write(command+" "+arg);
                            nodebw.newLine();
                            nodebw.flush();
                            String response = nodebr.readLine();
                            if (!Objects.equals(response, "ERROR")) {
                                bw.write(response);
                                found = true;
                                System.out.println("[N]: Found record at "+nodeIP+"! Response is: "+response);
                                break;
                            } else askedNodes.add(nodeIP);
                        }
                        synchronized (currentRequests) {
                            currentRequests.remove(request);
                        }
                        if (!found) {
                            bw.write("ERROR");
                            System.out.println("[N]: Could not find record "+arg);
                        }
                    }
                }
                break;
            }
            case "new-record": {
                String arg = request.substring(request.indexOf(' ') + 1);
                String[] split = arg.split(":");
                synchronized (key) {
                    key = split[0];
                    val = split[1];
                }
                bw.write("OK");
                break;
            }
            case "find-key": {
                String arg = request.substring(request.indexOf(' ') + 1);
                String wantedkey = arg;
                synchronized (key) {
                    if (key.equals(wantedkey)) {
                        System.out.println("[N]: I have "+wantedkey);
//                        if (nodeIPs.containsKey(helloIP)) {
//                            // to serwer wysyla zapytanie
//                            bw.write(nodeIPs.get(helloIP));
//                        } else {
                            // to klient wysyla zapytanie
                            bw.write(InetAddress.getLocalHost().getHostAddress()+":"+tcpport);
//                        }
                    } else {
                        System.out.println("[N]: Cannot find record "+wantedkey+", will ask other nodes!");
                        synchronized (currentRequests) {
                            currentRequests.add(request);
                        }
                        boolean found = false;
                        for (String nodeIP : nodeIPs.keySet()) {
                            if (askedNodes.contains(nodeIP)) continue;
                            System.out.println("[N]: Asking "+nodeIP);
                            Socket node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                            BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                            nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                            nodebw.newLine();
                            nodebw.flush();
                            nodebw.write(command+" "+arg);
                            nodebw.newLine();
                            nodebw.flush();
                            String response = nodebr.readLine();
                            if (!Objects.equals(response, "ERROR")) {
                                bw.write(response);
                                found = true;
                                System.out.println("[N]: Found record at "+nodeIP+"! Response is: "+response);
                                break;
                            } else askedNodes.add(nodeIP);
                        }
                        synchronized (currentRequests) {
                            currentRequests.remove(request);
                        }
                        if (!found) {
                            bw.write("ERROR");
                            System.out.println("[N]: Could not find record "+arg);
                        }
                    }
                }
                break;
            }
            case "terminate": {
                terminate = true;
                server.close();
                bw.write("OK");

                for (String nodeIP : nodeIPs.keySet()) {
                    System.out.println("[N]: Saying goodbye to "+nodeIP);
                    Socket node = new Socket(getHost(nodeIP), getPort(nodeIP));
                    BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                    nodebw.write("NODE-BYE "+nodeIPs.get(nodeIP));
                    nodebw.newLine();
                    nodebw.flush();
                    node.close();
                }

                bw.flush();
                hello.close();
                sockets.remove(hello);

                sockets.forEach(socket -> {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads.forEach(Thread::interrupt);
                System.out.println("[N]: All closed");
                return;
            }
            default:
                System.out.println("[N]: Error: could not process command: \""+command+"\"");
        }
        bw.flush();

        hello.close();
        sockets.remove(hello);

        System.out.println("[N]: Ended connection with "+helloIP);
    }

    public static synchronized boolean isRequestProcessed(String s) {
        return currentRequests.contains(s);
    }
}
