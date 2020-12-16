package com.iflytek.demo;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.Iterator;
import java.util.List;

public class JanusgraphApi {
    private static JanusGraph graph;

    @BeforeAll
    static void _1_create_graph_and_init_data() {
        final PropertiesConfiguration configuration = getGanusConfiguration();
        graph = JanusGraphFactory.open(configuration);
        // management = graph.openManagement();
        initSchema();
        initData();
    }

    private static PropertiesConfiguration getGanusConfiguration() {
        final PropertiesConfiguration p = new PropertiesConfiguration();
        p.setProperty("storage.backend", "inmemory");
        return p;
    }

    private static void initSchema() {
//        final VertexLabel vHumanLabel = management.makeVertexLabel("v_human").make();
        final PropertyKey pkName = graph.makePropertyKey("pk_name")
                .dataType(String.class).cardinality(Cardinality.SET).make();

        // 人这个点有一个属性是姓名
        final VertexLabel vHumanLabel = graph.makeVertexLabel("v_human").make();
        graph.addProperties(vHumanLabel, pkName);

        // 婚姻这条边有一个属性是结婚年龄, 同时是一个无向边
        final PropertyKey pkMarriedYear = graph.makePropertyKey("pk_married_year")
                .dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        final EdgeLabel eWifeLabel = graph.makeEdgeLabel("e_married")
                /*.unidirected()*/.multiplicity(Multiplicity.ONE2ONE).make();
        graph.addProperties(eWifeLabel, pkMarriedYear);

        // 朋友关系，无向，多对多，有一个属性是好友关系强度
        final PropertyKey pkTight = graph.makePropertyKey("pk_tight")
                .dataType(Double.class).cardinality(Cardinality.SINGLE).make();
        final EdgeLabel eFriendLabel = graph.makeEdgeLabel("e_friend")
                /*.unidirected()*/.multiplicity(Multiplicity.MULTI).make();
        graph.addProperties(eFriendLabel, pkTight);
    }

    private static void initData() {
        final String vHumanLabel = "v_human";
        final String eMarriedLabel = "e_married";
        final String eFriendLabel = "e_friend";
        final String pkMarriedYear = "pk_married_year";
        final String pkName = "pk_name";
        final String pkTight = "pk_tight";
        final Vertex vJimo = graph.addVertex(vHumanLabel);
        vJimo.property(pkName, "Jimo");
        final Vertex vLily = graph.addVertex(vHumanLabel);
        vLily.property(pkName, "Lily");

        final Vertex vHehe = graph.addVertex(vHumanLabel);
        vHehe.property(pkName, "Hehe");

        vJimo.addEdge(eMarriedLabel, vLily, pkMarriedYear, 10);
        vJimo.addEdge(eFriendLabel, vLily, pkTight, 0.5);
        vJimo.addEdge(eFriendLabel, vHehe, pkTight, 0.8);
        // vHehe.addEdge(eFriendLabel, vJimo, pkTight, 0.8);
    }

    @Test
    void _2_traversal_all() {
        final GraphTraversalSource g = graph.traversal();

        final List<Vertex> vertices = g.V().toList();
        for (Vertex v : vertices) {
            System.out.println(v.label() + ":" + v.property("pk_name"));
        }

        final List<Edge> edges = g.E().toList();
        for (Edge e : edges) {
            final Iterator<Property<Object>> properties = e.properties();
            System.out.print(e.label() + ":");
            while (properties.hasNext()) {
                System.out.print(properties.next() + ",");
            }
            System.out.println();
        }
    }

    @Test
    void _3_query() {
        final GraphTraversalSource g = graph.traversal();

        final Vertex myWife = g.V().has("pk_name", "Jimo").out("e_married").next();
        printVertex(myWife);

        final List<Vertex> jimoFriends = g.V().has("pk_name", "Jimo").out("e_friend").toList();
        System.out.println("Jimo friends:");
        for (Vertex heheFriend : jimoFriends) {
            printVertex(heheFriend);
        }
    }

    private void printVertex(Vertex v) {
        final Iterator<VertexProperty<Object>> ps = v.properties();
        System.out.print(v.label() + ":");
        while (ps.hasNext()) {
            final VertexProperty<Object> property = ps.next();
            System.out.print(property + ",");
        }
        System.out.println();
    }

    @AfterAll
    static void afterAll() {
        graph.close();
    }
}
