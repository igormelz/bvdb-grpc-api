package ru.openfs;

import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Value;
import io.quarkus.grpc.runtime.annotations.GrpcService;

@Path("/bvdb")
public class GreetingResource {

    @Inject
    @GrpcService("bvdb")
    DgraphGrpc.DgraphBlockingStub client;

    static String QUERY = "{data(func: type(Test)) {uid, testStr}}";

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String hello() {
        Request request = Request.newBuilder().setQuery(QUERY).build();
        return client.query(request).getJson().toStringUtf8();
    }

    @GET
    @Path("{str}")
    @Produces(MediaType.TEXT_PLAIN)
    public String insert(@PathParam("str") String str) {
        Set<NQuad> setNQuads = new HashSet<NQuad>();
        setNQuads.add(NQuad.newBuilder().setSubject("_:test").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("Test").build()).build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:test").setPredicate("testStr")
                .setObjectValue(Value.newBuilder().setStrVal(str).build()).build());
        Mutation mu = Mutation.newBuilder().addAllSet(setNQuads).setCommitNow(true).build();
        Request request = Request.newBuilder().addMutations(mu).setCommitNow(true).build();
        return client.query(request).getUidsOrDefault("test", "defaultValue");
    }

}