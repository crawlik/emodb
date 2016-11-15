package com.bazaarvoice.emodb.web.resources.system;

import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.system.SystemSource;
import io.dropwizard.jersey.params.LongParam;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Path ("/system/1")
@Produces (MediaType.APPLICATION_JSON)
public class SystemResource1 {

    private final SystemSource _systemSource;

    public SystemResource1(SystemSource systemSource) {
        _systemSource = checkNotNull(systemSource, "systemSource");
    }

    @POST
    @Path ("/stash-time/{id}")
    // @RequiresPermissions ("system|update")
    public SuccessResponse updateStashTime(@PathParam ("id") String id,
                                           @QueryParam ("timestamp") LongParam timestampInMillis) {
        checkArgument(timestampInMillis != null, "timestamp is required");

        _systemSource.updateStashTime(id, timestampInMillis.get());
        return SuccessResponse.instance();
    }

    @POST
    @Path ("/stash-time/{id}/delete")
    // @RequiresPermissions ("system|update")
    public SuccessResponse deleteStashTime(@PathParam ("id") String id) {
        _systemSource.deleteStashTime(id);
        return SuccessResponse.instance();
    }

    @GET
    @Path ("/stash-time/{id}")
    // @RequiresPermissions ("system|read")
    public Long getStashTime(@PathParam ("id") String id) {
        return _systemSource.getStashTime(id);
    }

    @GET
    @Path ("/stash-time/list")
    // @RequiresPermissions ("system|read")
    public Map<String, Long> listStashTimes() {
        return _systemSource.listStashTimes();
    }
}
