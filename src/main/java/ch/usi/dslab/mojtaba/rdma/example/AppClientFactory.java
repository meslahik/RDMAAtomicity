package ch.usi.dslab.mojtaba.rdma.example;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public class AppClientFactory implements RdmaEndpointFactory<AppClientEndpoint> {

    RdmaActiveEndpointGroup<AppClientEndpoint> endpointGroup;

    public  AppClientFactory(RdmaActiveEndpointGroup<AppClientEndpoint> _endpointGroup) {
        endpointGroup = _endpointGroup;
    }

    @Override
    public AppClientEndpoint createEndpoint(RdmaCmId rdmaCmId, boolean b) throws IOException {
        return new AppClientEndpoint(endpointGroup, rdmaCmId, b);
    }
}
