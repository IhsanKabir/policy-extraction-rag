# Labelling guide addendum (1 November 2026)

This rule has been added to the labelling guide. It applies to every record, including ones you have already labelled.

## A fixed fee and a percentage in the same rule

Some rules charge a fixed fee **and** a percentage of the fare, for example "Cancellation fee THB 500 plus 10% of base fare".

- **Penalty type:** `mixed`. It now means "two or more kinds of charge together", not only "fee + fare difference".
- **Penalty amount and currency:** the fixed fee, e.g. `500` / `THB`.
- **Penalty % (lost):** the percentage the passenger loses. For "refund 75%", that is `25`.
- **Fare difference:** leave it empty, unless the clause also mentions a fare difference.
- **Notes:** write two things:
  - what the percentage is taken from: `% of base fare` or `% of total fare`;
  - how the two combine: `fee plus %`, `whichever is higher` or `whichever is lower`.

| Clause | Type | Amount / Currency | % | Notes |
|---|---|---|---|---|
| "Cancellation fee THB 500 plus 10% of base fare" | `mixed` | 500 / THB | 10 | fee plus %; % of base fare |
| "25% of the fare or USD 50, whichever is higher" | `mixed` | 50 / USD | 25 | whichever is higher; % of total fare |
| "Refund 75% less an admin fee of AED 100" | `mixed` | 100 / AED | 25 | fee plus % |

If the fee and percentage change by time band, describe each band in the "Tiers / refund options" column instead.

If you can't tell whether an amount is a penalty or the price of an add-on (see example D in the guide), label only the penalty and explain in Notes.
