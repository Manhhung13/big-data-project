# ADR-05: Article identity and ticker mapping

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** Canonical URL exact dedup, many-to-many alias word boundary, mapping_version m1.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
