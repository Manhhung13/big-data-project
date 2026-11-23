# ADR-10: Hive priority

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** Spark session views pilot only; P1 needs persistent external metastore schema and partition tests.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
