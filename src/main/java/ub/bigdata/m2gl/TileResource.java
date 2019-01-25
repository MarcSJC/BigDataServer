package ub.bigdata.m2gl;

import java.awt.image.BufferedImage;
import java.io.File;
import javax.imageio.ImageIO;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

@Path("/")
public class TileResource {
	
	private static BufferedImage noimg;
	
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
		if (res != null) {
			return Response.ok(res).build();
		}
		else {
			//return Response.status(Status.NOT_FOUND).build();
			if (noimg == null) {
				ClassLoader cl = getClass().getClassLoader();
				File f = new File(cl.getResource("no.png").getFile());
				noimg = ImageIO.read(f);
			}
			return Response.ok(noimg).build();
		}
    }
	
}
