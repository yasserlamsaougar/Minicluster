package service

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import java.util.ArrayList

class AclService {

    fun giveAllPermissionsToZNode(zNodePath: String, zkConnection: String) {

        CuratorFrameworkFactory.newClient(zkConnection,
                ExponentialBackoffRetry(1000, 2)).use { client ->
            client.start()

            val perms = ArrayList<ACL>()
            perms.add(ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS))
            perms.add(ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE))

            client.create().withMode(CreateMode.PERSISTENT).withACL(perms).forPath(zNodePath)
        }

    }

}