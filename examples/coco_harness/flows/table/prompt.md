Turn the invoices under ./invoices into a table. First ask a planner to sample
the folder and propose the table's columns: short snake_case names with a
one-line description each. Then, with exactly those columns, have one worker
per invoice extract its row and write it to ./out as `<invoice file name>.json`
(for `inv-1.txt` write `inv-1.txt.json`): one JSON object whose keys are the
planned columns. Answer with the column list.
