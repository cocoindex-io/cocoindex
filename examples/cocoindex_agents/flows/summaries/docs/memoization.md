# Memoization

A function decorated with `@coco.fn(memo=True)` is skipped when its arguments
and its own code are unchanged since the last successful run. The engine keeps
one memo per component path, holding the fingerprint of the inputs and the
return value. There is no cache across paths: the same arguments at a different
path recompute.

Memoization is keyed on values, so a step that reruns but produces the same
output lets everything downstream of it hit. That is what makes a planner that
confirms last run's plan free for its dependents.
