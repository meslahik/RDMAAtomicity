package ch.usi.dslab.mojtaba.rdma.atomicity;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class ClientWrite {
    RdmaActiveEndpointGroup<AppClientEndpoint> endpointGroup;
    AppClientEndpoint endpoint;
    private String host;
    private int port;

    private int bufferSize = 4*100;
    private ByteBuffer sendBuf = ByteBuffer.allocateDirect(bufferSize);
    private IbvMr sendMr;

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
        System.out.println("ClientWrite::connected to address: " + addr);

        sendMr = endpoint.registerMemory(sendBuf).execute().free().getMr();
    }

    private void run() throws InterruptedException, IOException {

        //post receive operation to receive remote memory info
        postRecv();

        //wait for remote memory information
        endpoint.getWcEvents().take();

        //process received data
        processRecv();

        for (int i=0; i < 1000000; i++) {
            //change data in remote memory
            writeData(i);

            //wait for writing remote memory
            endpoint.getWcEvents().take();
//            System.out.println("ClientWrite::write completed");

//            //post read the remote data; issue a one-sided rdma read operation to fetch the content from remote buffer
//            postRead();
//
//            //wait for reading remote memory
//            endpoint.getWcEvents().take();
////            System.out.println("ClientWrite::read completed");
//
//            //Read changed data
//            readData();
        }
        System.out.println("finished!");

        //post receive operation to receive remote memory info
        postRecv();

        //wait for remote memory information
        endpoint.getWcEvents().take();

        //process received data
        processRecv();
    }

    private void postRecv() throws IOException{
//        IbvSge sgeRecv = new IbvSge();
//        sgeRecv.setAddr(recMr.getAddr());
//        sgeRecv.setLength(recMr.getLength());
//        sgeRecv.setLkey(recMr.getLkey());
//
//        LinkedList<IbvSge> sgeListRecv = new LinkedList<>();
//        sgeListRecv.add(sgeRecv);
//
//        IbvRecvWR recvWR = new IbvRecvWR();
//        recvWR.setSg_list(sgeListRecv);
//        recvWR.setWr_id(2001);
//
//        LinkedList<IbvRecvWR> wrListRecv = new LinkedList<>();
//        wrListRecv.add(recvWR);
//
//        endpoint.postRecv(wrListRecv).execute();
//        System.out.println("ClientWrite::initiated recv");
    }

    private void processRecv() {//processRecv
        endpoint.recBuf.clear();
        remBufAddr = endpoint.recBuf.getLong();
        remBufLength = endpoint.recBuf.getInt();
        remBufLkey = endpoint.recBuf.getInt();
        endpoint.recBuf.clear();
        System.out.println("ClientWrite::receiving rdma information, addr: " + remBufAddr + ", length: " + remBufLength + ", lkey= " + remBufLkey);
    }

//    private void postRead() throws IOException{
//        IbvSge sge = new IbvSge();
//        sge.setAddr(dataMr.getAddr());
//        sge.setLength(dataMr.getLength());
//        sge.setLkey(dataMr.getLkey());
//
//        LinkedList<IbvSge> sgeList = new LinkedList<>();
//        sgeList.add(sge);
//
//        IbvSendWR sendWR = new IbvSendWR();
//        sendWR.setWr_id(1001);
//        sendWR.setSg_list(sgeList);
//        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
//        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
//        sendWR.getRdma().setRemote_addr(remBufAddr);
//        sendWR.getRdma().setRkey(remBufLkey);
//
//        LinkedList<IbvSendWR> wrListSend = new LinkedList<>();
//        wrListSend.add(sendWR);
//
//        endpoint.postSend(wrListSend).execute();
//    }

//    private void readData() {
//        ByteBuffer dataBuf = endpoint.getDataBuf();
//        dataBuf.clear();
//        int num = dataBuf.asIntBuffer().get();
//        System.out.println("Array element: " + num);
//        for (int i=0; i < 99; i++) {
//            int num2 = dataBuf.asIntBuffer().get();
//            if (num2 != num)
//                System.out.println("A difference observed");
//        }
//    }

    private void writeData(int num) throws IOException {
        for (int i=0; i< bufferSize; i++)
            sendBuf.asIntBuffer().put(num);

        IbvSge sge = new IbvSge();
        sge.setAddr(sendMr.getAddr());
        sge.setLength(sendMr.getLength());
        sge.setLkey(sendMr.getLkey());

        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sge);

        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1002);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(remBufAddr);
        sendWR.getRdma().setRkey(remBufLkey);

        LinkedList<IbvSendWR> wrListSend = new LinkedList<>();
        wrListSend.add(sendWR);

        endpoint.postSend(wrListSend).execute();
    }

    private void close() throws IOException, InterruptedException{
        endpoint.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception{
        CmdLineCommon cmdLine = new CmdLineCommon("ClientWrite");

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
        ClientWrite client = new ClientWrite();
        client.launch(args);
    }
}
