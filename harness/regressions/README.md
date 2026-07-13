# Minimized regression traces

Seeded differential failures write minimized replay JSON files into this
directory. CI uploads the directory from the failing lane and includes this
manifest in every successful qualification artifact, so an empty trace list is
explicit rather than silently missing.

Each generated trace includes the randomized seed, query specification,
observed results, and an exact replay command. A trace remains a failing
regression fixture until the fix lands; the evidence ledger links to the
immutable run that produced it.
