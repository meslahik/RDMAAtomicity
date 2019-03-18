package ch.usi.dslab.mojtaba.rdma.atomicity;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class AppClientEndpoint extends RdmaActiveEndpoint {

    ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(10);

    ByteBuffer recBuf = ByteBuffer.allocateDirect(8+4+4);
    private IbvMr recMr;

    public AppClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }

    @Override
    public void init() throws IOException{
        super.init();

        recMr = registerMemory(recBuf).execute().free().getMr();

        IbvSge sgeRecv = new IbvSge();
        sgeRecv.setAddr(recMr.getAddr());
        sgeRecv.setLength(recMr.getLength());
        sgeRecv.setLkey(recMr.getLkey());

        LinkedList<IbvSge> sgeListRecv = new LinkedList<>();
        sgeListRecv.add(sgeRecv);

        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(4001);

        LinkedList<IbvRecvWR> wrListRecv = new LinkedList<>();
        wrListRecv.add(recvWR);

        postRecv(wrListRecv).execute();
        System.out.println("Client::initiated recv");
    }

    @Override
    public void dispatchCqEvent(IbvWC ibvWC) throws IOException {
        wcEvents.add(ibvWC);
    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        return wcEvents;
    }

}
