# Targets

A target is a place outputs live: a directory of files, a database table. You
declare target states (this file with this content, this row with these
values) and the engine compares them with what the previous run declared at
the same component path. New states are created, changed states are updated,
and states that are no longer declared are removed.

Declarations are applied atomically per component, so a reader of the target
never sees a half-updated set.
