package com.iflytek.demo;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphDemo {

    public static void main(String[] args) {
        try {
//            queryDemo1();
//            queryDemo2();
            queryDemo3();
//            queryDemo4();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }
    }

    public static void queryDemo1() throws Exception {
        Cluster cluster = Cluster.open("src/main/resources/conf/remote-objects.yaml");
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
        Object herculesAge = g.V().has("name", "hercules").values("age");
        System.out.println("Hercules is " + herculesAge + " years old.");
    }

    public static void queryDemo2() throws Exception {
        GraphTraversalSource g = traversal().withRemote("conf/remote-graph.properties");
        Object herculesAge = g.V().has("name", "john").values("age");
        System.out.println("Hercules is " + herculesAge + " years old.");
    }

    public static void queryDemo3() throws Exception {
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("192.168.56.101",8182,"g"));
        g.addV("test")
                .property("name","yuxj")
                .property("age",45).iterate();
        Object herculesAge = g.V().has("name", "yuxj").values("age").next();
        System.out.println("Hercules is " + herculesAge + " years old.");
        g.close();
    }

    public static void queryDemo4() throws Exception {
        // 出现未注册类的情况肯定是应用端反序列化时无法找到对应类。因此考虑如何将类注册到kryo中去
        // 注册类：org.janusgraph.graphdb.relations.RelationIdentifier到配置文件中去。
        Cluster cluster = Cluster.open("src/main/resources/conf/remote-objects.yaml");
        Client client = cluster.connect();

        Map<String,Object> params = new HashMap<>();
        params.put("x",4);
        List<Result> result = client.submit("[1,2,3,x]",params).all().get();
//        List<Result> result = client.submit("mgmt = graph.openManagement()").all().get();
        for(Result rs:result){
            System.out.println(rs.getObject());
        }

        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client, "g"));
        Object herculesAge = g.V().has("name", "yuxj").values("age");
        System.out.println("Hercules is " + herculesAge + " years old.");

        client.close();
        cluster.close();

    }

}
