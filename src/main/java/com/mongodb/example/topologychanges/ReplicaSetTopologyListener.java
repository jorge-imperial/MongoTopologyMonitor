package com.mongodb.example.topologychanges;

import com.mongodb.ReadPreference;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReplicaSetTopologyListener implements ClusterListener {

    private static Logger logger = LogManager.getLogger(ReplicaSetTopologyListener.class);

    private final ReadPreference readPreference;
    private boolean isWritable;
    private boolean isReadable;

    public ReplicaSetTopologyListener(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    @Override
    public void clusterOpening(final ClusterOpeningEvent clusterOpeningEvent) {
        logger.info("Cluster with unique client identifier opening" +
                clusterOpeningEvent.getClusterId());
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent clusterClosedEvent) {
        logger.info("Cluster with unique client identifier closed" +
                clusterClosedEvent.getClusterId());
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        if (!isWritable) {
            if (event.getNewDescription().hasWritableServer()) {
                isWritable = true;
                logger.info("Writable server available:" + event);
            }
        } else {
            if (!event.getNewDescription().hasWritableServer()) {
                isWritable = false;
                logger.info("No writable server available:" + event);
            }
        }

        if (!isReadable) {
            if (event.getNewDescription().hasReadableServer(readPreference)) {
                isReadable = true;
                logger.info("Readable server available:" + event);
            }
        } else {
            if (!event.getNewDescription().hasReadableServer(readPreference)) {
                isReadable = false;
                logger.info("No readable server available:" + event);
            }
        }
    }
}