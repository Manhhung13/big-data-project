# ADR-09: Consecutive event merge

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** Same symbol; elapsed event window_end gap <=10 minutes; rule_version in event_id.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
