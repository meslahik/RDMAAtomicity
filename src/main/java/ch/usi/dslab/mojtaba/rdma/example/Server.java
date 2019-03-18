package ch.usi.dslab.mojtaba.rdma.example;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class Server implements Serializable {
    RdmaActiveEndpointGroup<AppServerEndpoint> endpointGroup;
    private RdmaServerEndpoint<AppServerEndpoint> serverEndpoint;
    private AppServerEndpoint endpoint;
    private String host;
    private int port;

    public void init() throws Exception {
        endpointGroup =
                new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128); // TODO: Parameters?
        AppServerFactory factory = new AppServerFactory(endpointGroup);
        endpointGroup.init(factory);
        serverEndpoint = endpointGroup.createServerEndpoint();

        // binding
        InetAddress ipAddr = InetAddress.getByName(host);
        InetSocketAddress addr = new InetSocketAddress(ipAddr, port);
        serverEndpoint.bind(addr, 10);
        System.out.println("Server::bound to address " + addr.toString());

        //accept connection
        endpoint = serverEndpoint.accept();
        System.out.println("Server::Connection accepted");

        //put data in local buffer to be read/changed by client
        ByteBuffer dataBuf = endpoint.getDataBuf();
        dataBuf.asIntBuffer().put(121);
        dataBuf.clear();

        //send first message
        sendMrInfo();
    }

    public void sendMrInfo() throws IOException, InterruptedException{
        //fill the send buffer with the information about the memory region it can access
        IbvMr dataMr = endpoint.getDataMr();
        ByteBuffer sendBuf = endpoint.getSendBuf();
        sendBuf.putLong(dataMr.getAddr());
        sendBuf.putInt(dataMr.getLength());
        sendBuf.putInt(dataMr.getLkey());

        sendBuf.clear();

        //prepare scatter/gatherer and work request objects
        IbvMr sendMr = endpoint.getSendMr();
        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(sendMr.getAddr());
        sgeSend.setLength(sendMr.getLength());
        sgeSend.setLkey(sendMr.getLkey());
        LinkedList<IbvSge> sgeListSend = new LinkedList<>();
        sgeListSend.add(sgeSend);

        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(3001);
        sendWR.setSg_list(sgeListSend);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        LinkedList<IbvSendWR> wrListSend = new LinkedList<>();
        wrListSend.add(sendWR);

        //post the operation to send the message
        System.out.println("Server::sending message " + dataMr.getAddr() + ", " + dataMr.getLength() + ", " + dataMr.getLkey());
        endpoint.postSend(wrListSend).execute().free();

        //we have to wait for the CQ event, only then we know the message has been sent out
        endpoint.getWcEvents().take();
        System.out.println("Server::memory information sent");
    }

//    private void run() throws InterruptedException {
//        while(true) {
//            //wait for work completion // TODO: does remote write create work completions
//            endpoint.getWcEvents().take();
//            System.out.println("Server::memory written by remote process");
//
//            ByteBuffer dataBuf = endpoint.getDataBuf();
//            dataBuf.clear();
//            int num = dataBuf.asIntBuffer().get();
//            System.out.println("Server::Current value: " + num);
//        }
//    }

    private void close() throws IOException, InterruptedException{
        endpoint.close();
        serverEndpoint.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception{
        CmdLineCommon cmdLine = new CmdLineCommon("Server");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        host = cmdLine.getIp();
        port = cmdLine.getPort();

        init();
//        run();
    }

    public static void main(String[] argv) throws Exception{
        Server server = new Server();
        server.launch(argv);
    }
}
