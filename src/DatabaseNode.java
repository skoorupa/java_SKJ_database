import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class DatabaseNode {
    static boolean terminate = false;
    static int tcpport = 10000; // default
    static ArrayList<Thread> threads = new ArrayList<>();
    static ArrayList<Socket> sockets = new ArrayList<>();
    static ServerSocket server;
    static Integer key;
    static Integer val;
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
                    key = Integer.parseInt(split[0]);
                    val = Integer.parseInt(split[1]);
                    break;
                case "-connect":
                    connect_ips.add(args[++i]);
                    break;
            }
        }

        try {
            server = new ServerSocket(tcpport);
        } catch (IOException e) {
            System.err.println("[N]: "+tcpport+" is already in use, shutting down...");
            return;
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
            else command = request;
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
                int wantedkey = 0;
                try {
                    wantedkey = Integer.parseInt(arg);
                } catch (Exception e) {
                    System.out.println("[N]: "+arg+" is invalid");
                    bw.write("ERROR");
                    bw.flush();
                    hello.close();
                    return;
                }
                synchronized (key) {
                    if (key == wantedkey) {
                        System.out.println("[N]: Found record: "+wantedkey + ":" + val);
                        bw.write(wantedkey + ":" + val);
                    } else {
                        System.out.println("[N]: Cannot find record "+wantedkey+", will ask other nodes!");
                        synchronized (currentRequests) {
                            currentRequests.add(request);
                        }
                        boolean found = false;
                        ArrayList<String> failedNodes = new ArrayList<>();
                        for (String nodeIP : nodeIPs.keySet()) {
                            if (askedNodes.contains(nodeIP)) continue;
                            System.out.println("[N]: Asking "+nodeIP);
                            Socket node;
                            try {
                                node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            } catch (Exception e) {
                                System.out.println("[N]: Cannot get to node "+nodeIP+"! Removing it from the list");
                                failedNodes.add(nodeIP);
                                continue;
                            }
                            BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                            BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                            nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                            nodebw.newLine();
                            nodebw.flush();
                            nodebw.write(request);
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
                        failedNodes.forEach(node->nodeIPs.remove(node));
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
                int wantedkey = 0;
                int newvalue = 0;
                try {
                    wantedkey = Integer.parseInt(request.substring(request.indexOf(' ') + 1, request.indexOf(':')));
                    newvalue = Integer.parseInt(request.substring(request.indexOf(':') + 1));
                } catch (Exception e) {
                    System.out.println("[N]: "+arg+" is invalid");
                    bw.write("ERROR");
                    bw.flush();
                    hello.close();
                    return;
                }
                synchronized (key) {
                    if (key == wantedkey) {
                        System.out.println("[N]: I have record: "+wantedkey + ", changing value to: "+newvalue);
                        val = newvalue;
                        bw.write("OK");
                    } else {
                        System.out.println("[N]: Cannot find record "+wantedkey+", will ask other nodes!");
                        synchronized (currentRequests) {
                            currentRequests.add(request);
                        }
                        boolean found = false;
                        ArrayList<String> failedNodes = new ArrayList<>();
                        for (String nodeIP : nodeIPs.keySet()) {
                            if (askedNodes.contains(nodeIP)) continue;
                            System.out.println("[N]: Asking "+nodeIP);
                            Socket node;
                            try {
                                node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            } catch (Exception e) {
                                System.out.println("[N]: Cannot get to node "+nodeIP+"! Removing it from the list");
                                failedNodes.add(nodeIP);
                                continue;
                            }
                            BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                            BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                            nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                            nodebw.newLine();
                            nodebw.flush();
                            nodebw.write(request);
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
                        failedNodes.forEach(node->nodeIPs.remove(node));
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
                try {
                    Integer.parseInt(split[0]);
                    Integer.parseInt(split[1]);
                } catch (Exception e) {
                    System.out.println("[N]: "+arg+" is invalid");
                    bw.write("ERROR");
                    bw.flush();
                    hello.close();
                    return;
                }
                synchronized (key) {
                    key = Integer.parseInt(split[0]);
                    val = Integer.parseInt(split[1]);
                }
                bw.write("OK");
                break;
            }
            case "find-key": {
                String arg = request.substring(request.indexOf(' ') + 1);
                int wantedkey = 0;
                try {
                    wantedkey = Integer.parseInt(arg);
                } catch (Exception e) {
                    System.out.println("[N]: "+arg+" is invalid");
                    bw.write("ERROR");
                    bw.flush();
                    hello.close();
                    return;
                }
                synchronized (key) {
                    if (key == wantedkey) {
                        System.out.println("[N]: I have "+wantedkey);
                        if (nodeIPs.containsKey(helloIP)) {
                            // to serwer wysyla zapytanie
                            bw.write(nodeIPs.get(helloIP));
                        } else {
                            // to klient wysyla zapytanie
                            bw.write(hello.getInetAddress().getHostName()+":"+tcpport);
                        }
                    } else {
                        System.out.println("[N]: Cannot find record "+wantedkey+", will ask other nodes!");
                        synchronized (currentRequests) {
                            currentRequests.add(request);
                        }
                        boolean found = false;
                        ArrayList<String> failedNodes = new ArrayList<>();
                        for (String nodeIP : nodeIPs.keySet()) {
                            if (askedNodes.contains(nodeIP)) continue;
                            System.out.println("[N]: Asking "+nodeIP);
                            Socket node;
                            try {
                                node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            } catch (Exception e) {
                                System.out.println("[N]: Cannot get to node "+nodeIP+"! Removing it from the list");
                                failedNodes.add(nodeIP);
                                continue;
                            }
                            BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                            BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                            nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                            nodebw.newLine();
                            nodebw.flush();
                            nodebw.write(request);
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
                        failedNodes.forEach(node->nodeIPs.remove(node));
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
            case "get-max" : {
                synchronized (key) {
                    int maxkey = key;
                    int maxval = val;

                    System.out.println("[N]: my record is "+key+":"+val+", will ask other nodes!");
                    synchronized (currentRequests) {
                        currentRequests.add(request);
                    }
                    ArrayList<String> failedNodes = new ArrayList<>();
                    for (String nodeIP : nodeIPs.keySet()) {
                        if (askedNodes.contains(nodeIP)) continue;
                        askedNodes.add(nodeIP);

                        System.out.println("[N]: Asking "+nodeIP);
                        Socket node;
                        try {
                            node = new Socket(getHost(nodeIP), getPort(nodeIP));
                        } catch (Exception e) {
                            System.out.println("[N]: Cannot get to node "+nodeIP+"! Removing it from the list");
                            failedNodes.add(nodeIP);
                            continue;
                        }
                        BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                        BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                        nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                        nodebw.newLine();
                        nodebw.flush();
                        nodebw.write(request);
                        nodebw.newLine();
                        nodebw.flush();

                        String response = nodebr.readLine();
                        System.out.println("[N]: Got "+response);
                        if (!response.equals("ERROR")) {
                            int hiskey = Integer.parseInt(response.substring(0, response.indexOf(':')));
                            int hisval = Integer.parseInt(response.substring(response.indexOf(':')+1));
                            if (hisval > val) {
                                maxkey = hiskey;
                                maxval = hisval;
                            }
                        }
                    }
                    failedNodes.forEach(node->nodeIPs.remove(node));
                    synchronized (currentRequests) {
                        currentRequests.remove(request);
                    }
                    bw.write(maxkey+":"+maxval);
                    System.out.println("[N]: Sending "+maxkey+":"+maxval);
                }
                break;
            }
            case "get-min" : {
                synchronized (key) {
                    int minkey = key;
                    int minval = val;

                    System.out.println("[N]: my record is "+key+":"+val+", will ask other nodes!");
                    synchronized (currentRequests) {
                        currentRequests.add(request);
                    }
                    ArrayList<String> failedNodes = new ArrayList<>();
                    for (String nodeIP : nodeIPs.keySet()) {
                        if (askedNodes.contains(nodeIP)) continue;
                        askedNodes.add(nodeIP);

                        System.out.println("[N]: Asking "+nodeIP);
                        Socket node;
                            try {
                                node = new Socket(getHost(nodeIP), getPort(nodeIP));
                            } catch (Exception e) {
                                System.out.println("[N]: Cannot get to node "+nodeIP+"! Removing it from the list");
                                failedNodes.add(nodeIP);
                                continue;
                            }
                        BufferedReader nodebr = new BufferedReader(new InputStreamReader(node.getInputStream()));
                        BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                        nodebw.write("NODE-ASK "+nodeIPs.get(nodeIP));
                        nodebw.newLine();
                        nodebw.flush();
                        nodebw.write(request);
                        nodebw.newLine();
                        nodebw.flush();

                        String response = nodebr.readLine();
                        System.out.println("[N]: Got "+response);
                        if (!response.equals("ERROR")) {
                            int hiskey = Integer.parseInt(response.substring(0, response.indexOf(':')));
                            int hisval = Integer.parseInt(response.substring(response.indexOf(':')+1));
                            if (hisval < val) {
                                minkey = hiskey;
                                minval = hisval;
                            }
                        }
                    }
                    failedNodes.forEach(node->nodeIPs.remove(node));
                    synchronized (currentRequests) {
                        currentRequests.remove(request);
                    }
                    bw.write(minkey+":"+minval);
                    System.out.println("[N]: Sending "+minkey+":"+minval);
                }
                break;
            }
            case "terminate": {
                terminate = true;
                server.close();
                bw.write("OK");

                ArrayList<String> failedNodes = new ArrayList<>();
                for (String nodeIP : nodeIPs.keySet()) {
                    System.out.println("[N]: Saying goodbye to "+nodeIP);
                    Socket node;
                    try {
                        node = new Socket(getHost(nodeIP), getPort(nodeIP));
                    } catch (Exception e) {
                        System.out.println("[N]: Cannot get to node "+nodeIP+"! Removing it from the list");
                        failedNodes.add(nodeIP);
                        continue;
                    }
                    BufferedWriter nodebw = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));
                    nodebw.write("NODE-BYE "+nodeIPs.get(nodeIP));
                    nodebw.newLine();
                    nodebw.flush();
                    node.close();
                }
                failedNodes.forEach(node->nodeIPs.remove(node));

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
