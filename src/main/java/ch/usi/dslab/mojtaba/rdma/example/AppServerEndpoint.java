package ch.usi.dslab.mojtaba.rdma.example;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class AppServerEndpoint extends RdmaActiveEndpoint {
    private int bufferSize = 100;

    private ByteBuffer dataBuf;
    private IbvMr dataMr;

    private ByteBuffer sendBuf;
    private IbvMr sendMr;

    private ByteBuffer recBuf;
    private IbvMr recMr;

    ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(10);

    public AppServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }

    @Override
    public void init() throws IOException{
        super.init();

        dataBuf = ByteBuffer.allocateDirect(bufferSize);
        dataMr = registerMemory(dataBuf).execute().free().getMr();

        sendBuf = ByteBuffer.allocateDirect(bufferSize);
        sendMr = registerMemory(sendBuf).execute().free().getMr();

        recBuf = ByteBuffer.allocateDirect(bufferSize);
        recMr = registerMemory(recBuf).execute().free().getMr();

    }

    @Override
    public void dispatchCqEvent(IbvWC ibvWC) throws IOException {
        wcEvents.add(ibvWC);
    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        return wcEvents;
    }

    public ByteBuffer getDataBuf() {
        return dataBuf;
    }

    public IbvMr getDataMr() {
        return dataMr;
    }

    public ByteBuffer getSendBuf() {
        return sendBuf;
    }

    public IbvMr getSendMr() {
        return sendMr;
    }

    public ByteBuffer getRecBuf() {
        return recBuf;
    }

    public IbvMr getRecMr() {
        return recMr;
    }
}
