package com.bazaarvoice.emodb.web.resources.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.LongParam;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Produces (MediaType.APPLICATION_JSON)
public class CompactionControlResource1 {

    private final CompactionControlSource _compactionControlSource;

    public CompactionControlResource1(CompactionControlSource compactionControlSource) {
        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
    }

    @POST
    @Path ("/stash-time/{id}")
    @RequiresPermissions ("system|comp_control")
    public SuccessResponse updateStashTime(@PathParam ("id") String id,
                                           @QueryParam ("timestamp") LongParam timestampInMillisParam,
                                           @QueryParam ("placement") List<String> placements,
                                           @QueryParam ("expiredTimestamp") LongParam expiredTimestampInMillisParam,
                                           @QueryParam ("remote") @DefaultValue ("FALSE") BooleanParam remote) {
        checkArgument(timestampInMillisParam != null, "timestamp is required");
        checkArgument(!placements.isEmpty(), "Placement is required");
        checkArgument(expiredTimestampInMillisParam != null, "expired timestamp is required.");

        _compactionControlSource.updateStashTime(id, timestampInMillisParam.get(), placements, expiredTimestampInMillisParam.get(), remote.get());
        return SuccessResponse.instance();
    }

    @DELETE
    @Path ("/stash-time/{id}")
    @RequiresPermissions ("system|comp_control")
    public SuccessResponse deleteStashTime(@PathParam ("id") String id) {
        _compactionControlSource.deleteStashTime(id);
        return SuccessResponse.instance();
    }

    @GET
    @Path ("/stash-time/{id}")
    @RequiresPermissions ("system|comp_control")
    public String getStashTime(@PathParam ("id") String id) {
        StashRunTimeInfo stashTimeInfo = _compactionControlSource.getStashTime(id);
        return (stashTimeInfo != null) ? stashTimeInfo.toString() : null;
    }

    @GET
    @Path ("/stash-time")
    @RequiresPermissions ("system|comp_control")
    public Map<String, StashRunTimeInfo> getStashTimes() {
        // return Maps.transformValues(_compactionControlSource.listStashTimes(), value -> (value != null) ? value.toString() : null);
        return _compactionControlSource.getStashTimes();
    }
}
