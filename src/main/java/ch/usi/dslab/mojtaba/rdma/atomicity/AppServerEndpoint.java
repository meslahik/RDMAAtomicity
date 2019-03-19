package ch.usi.dslab.mojtaba.rdma.atomicity;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class AppServerEndpoint extends RdmaActiveEndpoint {

    ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(10);

    public AppServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }

    @Override
    public void dispatchCqEvent(IbvWC ibvWC) throws IOException {
        wcEvents.add(ibvWC);
    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        return wcEvents;
    }

}
