package ch.usi.dslab.mojtaba.rdma.example;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class Client {
    RdmaActiveEndpointGroup<AppClientEndpoint> endpointGroup;
    AppClientEndpoint endpoint;
    private String host;
    private int port;

    private long remBufAddr;
    private int remBufLength;
    private int remBufLkey;

    public void init() throws Exception {
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
    }

    private void run() throws InterruptedException, IOException {

        //post receive operation to receive remote memory info
        postRecv(); //TODO: no problem with post receive after connection?

        //wait for remote memory information
        endpoint.getWcEvents().take();

        //process received data
        processRecv();

        //post read the remote data; issue a one-sided rdma read operation to fetch the content from remote buffer
        postRead();

        //wait for reading remote memory
        endpoint.getWcEvents().take();
        System.out.println("ClientWrite::read completed");

        //read the data
        readData();

        //change data in remote memory
        writeData();

        //wait for writing remote memory
        endpoint.getWcEvents().take();
        System.out.println("ClientWrite::write completed");

        //post read the remote data; issue a one-sided rdma read operation to fetch the content from remote buffer
        postRead();

        //wait for reading remote memory
        endpoint.getWcEvents().take();
        System.out.println("ClientWrite::read completed");

        //Read changed data
        readData();
    }

    private void postRecv() throws IOException{
        IbvMr recMr = endpoint.getRecMr();

        IbvSge sgeRecv = new IbvSge();
        sgeRecv.setAddr(recMr.getAddr());
        sgeRecv.setLength(recMr.getLength());
        sgeRecv.setLkey(recMr.getLkey());

        LinkedList<IbvSge> sgeListRecv = new LinkedList<>();
        sgeListRecv.add(sgeRecv);

        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(2001);

        LinkedList<IbvRecvWR> wrListRecv = new LinkedList<>();
        wrListRecv.add(recvWR);

        System.out.println("ClientWrite::initiated recv");
        endpoint.postRecv(wrListRecv).execute();
    }

    private void processRecv() {
        ByteBuffer recvBuf = endpoint.getRecBuf();
        recvBuf.clear();
        remBufAddr = recvBuf.getLong();
        remBufLength = recvBuf.getInt();
        remBufLkey = recvBuf.getInt();
        recvBuf.clear();
        System.out.println("ClientWrite::receiving rdma information, addr: " + remBufAddr + ", length: " + remBufLength + ", lkey= " + remBufLkey);
    }

    private void postRead() throws IOException{
        IbvMr dataMr = endpoint.getDataMr();

        IbvSge sge = new IbvSge();
        sge.setAddr(dataMr.getAddr());
        sge.setLength(dataMr.getLength());
        sge.setLkey(dataMr.getLkey());

        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sge);

        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(1001);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(remBufAddr);
        sendWR.getRdma().setRkey(remBufLkey);

        LinkedList<IbvSendWR> wrListSend = new LinkedList<>();
        wrListSend.add(sendWR);

        endpoint.postSend(wrListSend).execute();
    }

    private void readData(){
        ByteBuffer dataBuf = endpoint.getDataBuf();
        dataBuf.clear();
        int num = dataBuf.asIntBuffer().get();
        System.out.println("ClientWrite::remote memory data: " + num);
    }

    private void writeData() throws IOException{
        ByteBuffer sendBuf = endpoint.getSendBuf();
        sendBuf.asIntBuffer().put(168);
        IbvMr mr = endpoint.getSendMr();

        IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(mr.getLength());
        sge.setLkey(mr.getLkey());

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

        init();
        run();
        close();
    }

    public static void main(String[] args) throws Exception{
        Client client = new Client();
        client.launch(args);
    }


}
