package com.bazaarvoice.emodb.web.system;

import java.util.Map;

public interface SystemSource {

    void updateStashTime(String id, long timestamp);

    void deleteStashTime(String id);

    Long getStashTime(String id);

    Map<String, Long> listStashTimes();

}
