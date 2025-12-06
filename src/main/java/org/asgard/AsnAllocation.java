package org.asgard;

import java.time.LocalDate;

public record AsnAllocation(long startAsn, long count, String registry, String country, String status,
                            LocalDate allocationDate) {

    public AsnAllocation {
        if (count <= 0) throw new IllegalArgumentException("Allocation count must be positive");
    }

    public long endAsn() {
        return startAsn + count - 1;
    }
}
