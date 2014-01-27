package com.addthis.hydra.store.db;

public enum CloseOperation {

    NONE(false, false), TEST(true, false), REPAIR(true, true);

    private final boolean test;
    private final boolean repair;

    CloseOperation(boolean test, boolean repair) {
        this.test = test;
        this.repair = repair;
    }

    public boolean testIntegrity() {
        return test;
    }

    public boolean repairIntegrity() {
        return repair;
    }


}
