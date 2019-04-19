# Data Pipelines: Clojure vs Python
Grammarly wrote [this](https://tech.grammarly.com/blog/building-etl-pipelines-with-clojure)
excellent blog post on building ETL pipelines in Clojure. I've primarily written
ETL pipelines in Python and wanted to see how a similar pipeline would compare
in Python.

As of writing this I'm Clojure curious, but a noob so largely lifted
the Clojure code from the Grammarly post. There are many aspects of Clojure that
appeal to me, and I was optimistic that I would finish this exercise convinced
Clojure would have definite advantages to writing ETL jobs over Python.  However,
the results are more mixed than expected.


#### Clojure results
```
$ lein run

--- Time how we do for a single file ---

Lazy:
"Elapsed time: 946.714266 msecs"

Parallel:
"Elapsed time: 840.418467 msecs"

--- What if we run on 10 files ---

Lazy:
"Elapsed time: 8832.775676 msecs"

Parallel:
"Elapsed time: 2281.054373 msecs"
```

#### Python results

```
$ python3 python/pipeline.py

--- Time how we do for a single file ---

Lazy:
Elapsed time: 900.0811576843262 msecs

Parallel:
Elapsed time: 995.0668811798096 msecs

--- What if we run on 10 files ---

Lazy:
Elapsed time: 9346.51803970337 msecs

Parallel:
Elapsed time: 2756.990909576416 msecs
```

## Thoughts

I expected Clojure to be order of magnitudes faster than Python and was surprised
that is only marginally faster in most cases. Why is this? Is there something in
the Clojure that is making it much less efficient than it could be?

The Clojure program was slightly smaller which is a plus and the general composbility in a more functional paradigm is nice. However, in my current
context this is not compelling enough to consider switching writing pipelines
in Clojure vs Python. Is there something I'm missing? Are there compelling
reasons I'm missing?
