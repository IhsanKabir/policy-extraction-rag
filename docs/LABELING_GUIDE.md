# How to fill the labelling form (v1b)

## Golden rules
1. **Only what the CLAUSE (left box) says.** The full chunk is only there to help you understand the clause. If a value isn't in the clause, leave its field empty.
2. **Empty is better than a guess.** An empty field means "the clause doesn't say". That is a correct answer.
3. **Same clause text → same label,** every time.
4. **Meaning may come from the chunk; values may not.** If the clause only makes sense with its context (e.g. a bare row of a refund table), use the chunk to choose the **topic and action**, and write `Topic inferred from the surrounding chunk` in Notes. Numbers, time windows and percentages must still be in the clause. Text that belongs to a neighbouring row (e.g. a stray `0%` at the start) is ignored and mentioned in Notes.

## Step 0: label or Skip?
Press **Skip** if the clause states no rule: website menus, cookie or copyright text, contact details, app ads, or a cut-off fragment ("You can also cancel your booking up to", "Upgrade:").
Otherwise label it, even if it's off-topic (baggage, meals, documents). Those get topic `other`.
- **Cut-off clauses:** skip only if you **can't tell what the rule is** ("to this channel dues immediately at change", "...roundtrip booking and"). If the rule is still fully clear despite the cut, **label it with the same label as the complete version**, and say it's cut off in Notes.
- **Fragment + complete rule** in one clause: label the complete rule; mention the fragment in Notes.
- **Two complete rules** in one clause: label the one about permission or fees; put the other in Notes.
- **Channel headings and pure how-to** ("Call Center 1318 Simple and easy", "Just call us and inform our staff"): Skip. A channel line that states a permission, fee, deadline or obligation is labelled.

## Step 1: Classification
**Policy Topic** (pick one):
| Topic | Use when the clause is about… |
|---|---|
| `refund` | getting money back, including "Refund/Cancellation" written together |
| `cancellation` | cancelling, **only** when the clause treats it separately from refund ("cancellation allowed but non-refundable") |
| `reissue_change` | changing date, flight, route or upgrade; reissuing a ticket |
| `no_show` | not showing up for the flight |
| `go_show` | turning up for an earlier/later flight at the airport |
| `waiver` | a fee waived because of a disruption or airline exception |
| `other` | everything else: name change, baggage, validity, documents, meals, seats, loyalty |

**Topic = the transaction the passenger is doing, not the words in the clause.** Leftover value when changing to a cheaper flight ("we may refund the difference", "surplus will be forfeited") is `reissue_change`, as in ATPCO Cat 31 residual rules. `refund` (Cat 33) is for cancelling a ticket to get money back.

**Action:**
| Action | Use when… |
|---|---|
| `allowed` | it's permitted and **nothing** is attached (no fee, no time limit, no condition) |
| `conditional` | it's permitted **but** with a fee, fare difference, time limit or condition |
| `not_allowed` | "not permitted", "not available", "non-refundable" |
| `unknown` | the clause doesn't say whether it's allowed (e.g. "Hand baggage: 7 kg") |

## Step 2: Penalty / Financial
**Penalty Type:**
| Type | Clause says… |
|---|---|
| `free` | nothing is charged at all |
| `fare_difference_only` | "free of charge, charge difference in fare" or just "charge difference in fare" |
| `mixed` | a change fee **and** the fare difference |
| `fixed_amount` | one fixed fee, e.g. "THB 225 per passenger" |
| `percentage` | a percentage is lost or kept, e.g. "refund 75%" |
| `forfeiture` | the money or residual value is lost entirely |
| `tax_only` | only taxes are refunded or charged |
| `unknown` | there is a charge but the clause doesn't say what kind |
| *(empty)* | the clause mentions no charge at all |

- **Penalty Amount / Currency:** only when the clause gives **one** fee, e.g. `225` / `THB`. Use the currency code (THB, USD, AED, INR, MYR, QAR).
- **Penalty Percent:** what the passenger **loses**, not what they get back. Refund 75% → `25`.
- **`fare_difference_only` vs just Fare Difference = true:** use `fare_difference_only` only when the clause presents the fare difference as the **whole** charge (e.g. a fee-table cell: "Free of charge, charge difference in fare"). If a sentence just says the passenger pays the fare difference, and other fees may apply, leave Penalty Type **empty** and set Fare Difference to `true`.
- **Fare Difference Required:** `true` only if the clause says fare difference applies; `false` only if it explicitly says none. Otherwise empty.
- **Tax Refund Rule:** short phrase, only if taxes are mentioned: `taxes refundable`, `airport tax included in refund`, `taxes not refundable`.

## Step 3: Timing / Eligibility
- **Time Window:** `6 hours`, `24 hours`, `7 days`, `31 days`; ranges `8-30 days`, `24 hours-7 days`. Lower-case, number plus unit.
- **Before/After Departure:** `before_departure`, `after_departure`, `both`, `not_applicable`, or empty if not stated.
- **Eligibility Conditions** (one per line): who or what qualifies, as a real condition, e.g. `airline reschedules the flight by more than 4 hours`, `unused tickets only`. **Don't** repeat the clause and **don't** write "if any".
- **Exceptions** (one per line): who is excluded, e.g. `group bookings`, `reward tickets`. Don't put the same thing in both fields.

## Step 4: Penalty Tiers (when the rule changes by time band)
If the clause gives **different** fees or percentages for different time bands, **leave the top-level penalty and time fields empty** and add one **+ Tier** row per band:
- Time Window, Before/After, Penalty Type, Amount **or** Percent, Currency, and Notes with the original wording ("refund 75%", "more than 31 days").
- Fill only the bands whose values are in the clause. If a band's value is missing from the clause, leave that value empty.
- **Tiers are also for other variation in the same rule,** e.g. domestic vs international fees. Leave the tier's time window empty and name the variant in its notes ("domestic").
- **Several currencies for the same fee:** put the airline's own currency in Amount/Currency (THB for Thai carriers, AED for Air Arabia, MYR for AirAsia, QAR for Qatar, INR for Indian carriers, USD if it's the only one) and list the others in notes ("also USD 27 / CNY 182").

## Step 5: Refund Payout Options (how money comes back)
Use **+ Refund option** only when the clause says how the refund is paid:
- **Method:** `original_payment`, `travel_credit` (voucher, credit shell, wallet) or `cash`.
- **Allowed:** true/false. **Fee:** type, amount or percent, if stated.
- **Time Window:** validity, e.g. `365 days`. **Notes:** e.g. "credit shell in passenger's name".

## Step 6: Notes
Use notes for anything a reader needs: ambiguity, the price of an add-on ("Nok Refund 200 THB, not a penalty"), or when the clause mixes two fare types.

## The form helps you (guardrails, 2026-10-28)
- **Locked (greyed) fields** can't be used in the current situation. Once any Penalty Tier has a value, the top-level Time Window, Penalty Amount, Currency and Penalty % lock. Penalty Type decides which value fields open: `percentage` → %, `fixed_amount` → amount + currency, `mixed` / `unknown` → both, everything else → none. Hover a locked field to see why.
- **`fare_difference_only` or `mixed`** sets Fare Difference to `true` automatically.
- **Time windows tidy themselves** when you leave the box: `08-30 Days` → `8-30 days`, `24 Hrs. - 7 Days` → `24 hours-7 days`, `Within 24 Hours` → `24 hours` (with "within 24 hours" added to the notes). Currencies become upper case.
- **Before saving**, a pop-up lists anything suspicious: a number not in the clause, a wrong format, an eligibility line that copies the clause, a missing topic or action. **Cancel** to fix, or **OK** to save anyway when you're sure (e.g. a percentage you calculated: refund 75% → 25 is accepted automatically).

## Worked examples
**A. `Refund/Cancellation: Not Available`**
topic `refund` · action `not_allowed` · everything else empty.

**B. `Changes in Flight: Charge fee of change and difference in fare, if any`**
topic `reissue_change` · action `conditional` · penalty type `mixed` · fare difference `true` · rest empty (the fee amount isn't in the clause).

**C. `Changes condition: 3 hours before departure`**
topic `reissue_change` · action `conditional` · time window `3 hours` · before/after `before_departure`.

**D. Nok Refund table: `... Domestic 200 THB above 31 Days 100% 8 - 30 Days 75% Domestic (Connecting Flight) 400 THB 24 Hrs. - 7 Days 50% Less than 24 Hrs.`**
topic `refund` · action `conditional` · penalty type `percentage` · tax rule `airport tax included in refund` (if the clause says so) · before/after `before_departure` · top-level amount, percent and time window **empty** · tiers:
| Time window | Penalty % | Notes |
|---|---|---|
| `31 days` | `0` | refund 100%, more than 31 days |
| `8-30 days` | `25` | refund 75% |
| `24 hours-7 days` | `50` | refund 50% |
| `24 hours` | *(empty)* | less than 24 hrs; % not in this clause |

Notes: `Nok Refund add-on price 200 THB domestic / 400 THB domestic connecting. Not a penalty.`

**E. `Hand baggage: 7 Kg`**
topic `other` · action `unknown` · everything else empty.

**F. `Get help Or contact us at (0084 938) 70 26 66 ...`**
→ **Skip**.
