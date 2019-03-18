package ch.usi.dslab.mojtaba.rdma.atomicity;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public class AppServerFactory implements RdmaEndpointFactory<AppServerEndpoint> {

    private RdmaActiveEndpointGroup<AppServerEndpoint> endpointGroup;

    public AppServerFactory(RdmaActiveEndpointGroup<AppServerEndpoint> _endpointGroup) {
        endpointGroup = _endpointGroup;
    }

    @Override
    public AppServerEndpoint createEndpoint(RdmaCmId rdmaCmId, boolean b) throws IOException {
        return new AppServerEndpoint(endpointGroup, rdmaCmId, b);
    }
}
