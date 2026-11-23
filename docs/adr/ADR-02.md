# ADR-02: Minute timestamp semantics

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** Treat bar_start as [start,end); do not interchange bar_end without contract migration.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
