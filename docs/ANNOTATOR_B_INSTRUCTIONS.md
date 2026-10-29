# Instructions for the second annotator

Thank you for helping. You will read 50 short pieces of airline text (fare rules and policy pages) and record what each one says in a form. Your answers are compared with another person's answers to measure how consistently people read these rules. There are no trick questions, and disagreements are useful results, not mistakes.

**Time:** about 2–3 hours. You can stop and continue later; every saved record is kept.

## Rules (these make the result valid)

1. **Work alone.** Don't discuss the records with anyone, including the project owner, until you have finished all 50.
2. **No AI tools.** Don't paste text into ChatGPT, Claude, Copilot or similar, and don't ask one for help with a rule.
3. **Don't look anything up** on the airline's website. Label only what the text on screen says.
4. **Use only the labelling guide** (`docs/LABELING_GUIDE.md`, printed or open beside you). Read it once fully before you start.
5. **When unsure,** choose your best answer and write the reason in **Notes**. Don't leave a record half done.

## How to label

1. The project owner starts the tool for you and opens `http://127.0.0.1:8000/labeling`.
2. The **left box** is the clause to label. The full surrounding text is shown only to help you understand it.
3. Decide first: does the clause state a rule? If not (menus, contact details, cut-off fragments), press **Skip**. The guide's "Step 0" explains this.
4. Otherwise fill in the form: topic, action, then any penalty, time limit or conditions **the clause itself** states. Empty fields mean "the clause doesn't say", and that is often the right answer.
5. Press **Save** and move to the next record.

## When you're done

Tell the project owner. Please don't go back and change answers after you've finished, and don't compare answers with anyone afterwards until the results are written up.

## What happens to your answers

They are stored only in a separate file on this computer (`data/labeling_iaa_b.db`). They are used to compute agreement statistics for a research paper, and you can be acknowledged by name in the paper if you wish.
