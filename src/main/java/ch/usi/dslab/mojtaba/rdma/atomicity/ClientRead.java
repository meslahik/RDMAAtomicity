package ch.usi.dslab.mojtaba.rdma.atomicity;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class ClientRead {
    RdmaActiveEndpointGroup<AppClientEndpoint> endpointGroup;
    AppClientEndpoint endpoint;
    private String host;
    private int port;

    private int bufferSize = 4*100;
    private ByteBuffer dataBuf = ByteBuffer.allocateDirect(bufferSize);
    private IbvMr dataMr;

    private long remBufAddr;
    private int remBufLength;
    private int remBufLkey;

    public void initialize() throws Exception {
        endpointGroup =
                new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        AppClientFactory factory = new AppClientFactory(endpointGroup);
        endpointGroup.init(factory);
        endpoint = endpointGroup.createEndpoint();

        //connect
        InetAddress ipAddr = InetAddress.getByName(host);
        InetSocketAddress addr = new InetSocketAddress(ipAddr, port);
        endpoint.connect(addr, 1000);
        System.out.println("ClientRead::connected to address: " + addr);

        dataMr = endpoint.registerMemory(dataBuf).execute().free().getMr();
    }

    private void run() throws InterruptedException, IOException {

        //post receive operation to receive remote memory info
        postRecv();

        //wait for remote memory information
        endpoint.getWcEvents().take();

        //process received data
        processRecv();

        for (int i=0; i < 10000000; i++) {
            //post read the remote data; issue a one-sided rdma read operation to fetch the content from remote buffer
            postRead();

            //wait for reading remote memory
            endpoint.getWcEvents().take();

            //Read changed data
            readData();
        }
        System.out.println("ClientRead::finished!");
    }

    private void postRecv() throws IOException{

    }

    private void processRecv() {//processRecv
        endpoint.recBuf.clear();
        remBufAddr = endpoint.recBuf.getLong();
        remBufLength = endpoint.recBuf.getInt();
        remBufLkey = endpoint.recBuf.getInt();
        endpoint.recBuf.clear();
        System.out.println("ClientRead::receiving rdma information, addr: " + remBufAddr + ", length: " + remBufLength + ", lkey= " + remBufLkey);
    }

    private void postRead() throws IOException {
        IbvSge sge = new IbvSge();
        sge.setAddr(dataMr.getAddr());
        sge.setLength(dataMr.getLength());
        sge.setLkey(dataMr.getLkey());

        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sge);

        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(3001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(remBufAddr);
        sendWR.getRdma().setRkey(remBufLkey);

        LinkedList<IbvSendWR> wrListSend = new LinkedList<>();
        wrListSend.add(sendWR);

        endpoint.postSend(wrListSend).execute();
    }

    private void readData() {
        dataBuf.clear();
        int num = dataBuf.asIntBuffer().get();
        System.out.println("Array element: " + num);
        for (int i=1; i < bufferSize; i++) {
            int num2 = dataBuf.asIntBuffer().get();
            if (num2 != num)
                System.out.println("A difference observed");
        }
    }

    private void close() throws IOException, InterruptedException{
        endpoint.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception{
        CmdLineCommon cmdLine = new CmdLineCommon("ClientRead");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        host = cmdLine.getIp();
        port = cmdLine.getPort();

        initialize();
        run();
        close();
    }

    public static void main(String[] args) throws Exception{
        ClientRead client = new ClientRead();
        client.launch(args);
    }
}
