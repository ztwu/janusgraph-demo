package com.iflytek.demo;

import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

// janusgraph远程服务
public class JanusGraphDemo {

    public static void main(String[] args) {
        try {
//            queryDemo1();
//            queryDemo2();
//            queryDemo3();
//            queryDemo4();
            queryDemo5();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }
    }

    public static void queryDemo1() throws Exception {
        Cluster cluster = Cluster.open("src/main/resources/conf/remote-objects.yaml");
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
        Object herculesAge = g.V().has("name", "yuxj").values("age").toList();
        System.out.println("Hercules is " + herculesAge + " years old.");
        g.close();
        cluster.close();
    }

    public static void queryDemo2() throws Exception {
        GraphTraversalSource g = traversal().withRemote("conf/remote-graph.properties");
        Object herculesAge = g.V().has("name", "yuxj").values("age").toList();
        System.out.println("Hercules is " + herculesAge + " years old.");
        g.close();
    }

    public static void queryDemo3() throws Exception {
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("192.168.56.101",8182,"g"));
        g.addV("test")
                .property("name","yuxj")
                .property("age",45).iterate();
        Object herculesAge = g.V().has("name", "yuxj").values("age").toList();
        System.out.println("Hercules is " + herculesAge + " years old.");
        g.close();
    }

//    提交gremlin-sql
    public static void queryDemo4() throws Exception {
        Cluster cluster = Cluster.open("src/main/resources/conf/remote-objects.yaml");
        Client client = cluster.connect();

        Map<String,Object> params = new HashMap<>();
        params.put("x",4);
        List<Result> result = client.submit("g.V().toList()").all().get();
//        List<Result> result = client.submit("[1,2,3,x]",params).all().get();
        for(Result rs:result){
            System.out.println(rs.getObject());
        }

        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client, "g"));
        Object herculesAge = g.V().has("name", "yuxj").values("age").toList();
        System.out.println("Hercules is " + herculesAge + " years old.");
        client.close();
        cluster.close();

    }

    //    提交gremlin-sql
    public static void queryDemo5() throws Exception {
        Cluster cluster = Cluster.open("src/main/resources/conf/remote-objects.yaml");
        Client client = cluster.connect();

//      使用模板来创建新图减少重复输入形同配置，这里不需要添加graph.graphname配置项
        String gremlin_sql =
                "map = new HashMap<String, Object>();" +
                "map.put(\"storage.backend\", \"hbase\");" +
                "map.put(\"storage.hostname\", \"172.16.0.10\");" +
                "map.put(\"index.search.backend\", \"elasticsearch\");" +
                "map.put(\"index.search.hostname\", \"172.18.0.10\");" +
                "ConfiguredGraphFactory.updateConfiguration(\"graphztwu10\",new MapConfiguration(map));" +
//                "ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));" +
                "ConfiguredGraphFactory.create(\"graphztwu10\");" +
                "ConfiguredGraphFactory.open(\"graphztwu10\");";
        CompletableFuture<ResultSet> rs = client.submitAsync(gremlin_sql);
        System.out.println(rs.isCompletedExceptionally());
        rs.whenCompleteAsync((result, e) -> {
                    System.out.println(result);
                    System.out.println(e);
                });

        client.close();
        cluster.close();

    }

}
