package ub.bigdata.m2gl;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.ToolRunner;

import ub.bigdata.m2gl.HBaseLink.HBaseProg;

@Path("/")
public class TileResource {
	
	@GET
	@Path("tile/{z}/{x}/{y}")
    @Produces("image/png")
	//@Produces("text/plain")
    public Response getTile(@PathParam("z") String z, @PathParam("x") String x, @PathParam("y") String y) throws Exception {
		byte[] res;
		ToolRunner.run(HBaseConfiguration.create(), new HBaseLink.HBaseProg(), null);
		String row = z + "/" + x + "/" + y;
		res = HBaseLink.HBaseProg.get(row);
		//res = get(row);
		if (res != null) 
			return Response.ok(res).build();
		else 
			return Response.status(Status.NOT_FOUND).build();
    }
	
}
