package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.store.DataStore;

public class NewPanosTest {
    private String getStatisticsValue(String key) {
        return DataStore.statisticsRedis().getValue(key).get();
    }
}
